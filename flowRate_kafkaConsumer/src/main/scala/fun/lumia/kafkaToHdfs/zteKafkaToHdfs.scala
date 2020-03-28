package fun.lumia.kafkaToHdfs

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import fun.lumia.Constant
import fun.lumia.bean.scalaClass.B2BModel
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils.{tranTimeToLong, tranTimeZte}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}


object zteKafkaToHdfs extends SparkTool {

  override def run(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Durations.minutes(5))
    ssc.checkpoint(Constant.SPARK_STREAMING_CHECKPOINT_PATH + "/" + Constant.ZTE_KAFKA_B2B_TOPIC_NAME)
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Constant.KAFKA_BOOTSTRAP_SERVERS,
      ConsumerConfig.GROUP_ID_CONFIG -> Constant.GROUP_ID_CONFIG,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> Constant.AUTO_OFFSET_RESET_CONFIG
    )

    /**
     * direct模式  主动拉取数据
     *
     * 当计算完之后才会更新偏移量，将偏移量存到hdfs
     *
     * DS分区数 = topic  partition数
     *
     */
    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(Constant.ZTE_KAFKA_B2B_TOPIC_NAME)
    ).map(_._2)

    ds.foreachRDD(zteSourceRDD => {
      if (!zteSourceRDD.isEmpty()) {
        val sqlContext = sql
        import sqlContext.implicits._
        // filter 不规则数据
        val zteRDD = zteSourceRDD.filter(line => {
          val json = JSON.parseObject(line)
          val message = json.getString("message")
          val messageSplits = message.split("\"")
          var flag = true
          try {
            val fisrttimes = messageSplits(2).trim.split(" ")
            fisrttimes(0).toDouble
            fisrttimes(1).drop(1)
            val httpstatus_filesize = messageSplits(4).trim.split(" ")
            httpstatus_filesize(0).trim
            httpstatus_filesize(1).trim.toDouble
            messageSplits(9).trim.toDouble
          } catch {
            case es: Exception =>
              flag = false
          }
          flag && messageSplits(0).trim.contains(".") && Pattern.matches(".*\\.(com|cn)", messageSplits(1).trim)
        }).map(line => { // 取出需要的字段并转换成DataFrame，最后以parquet格式存储
          val json = JSON.parseObject(line)
          /**
           *
           */
          val tagsArray = json.getJSONArray("tags")
          var city = "未知城市"
          if (!tagsArray.isEmpty) {
            city = tagsArray.get(0).toString.trim()
          }
          val messageSplits = json.getString("message").split("\"")
          val fisrttimes = messageSplits(2).trim.split(" ")
          val server_ip = messageSplits(0).trim
          val reqdomain = messageSplits(1).trim
          val firsttime = fisrttimes(0).toDouble
          val timestamp = tranTimeToLong(tranTimeZte(fisrttimes(1).drop(1)))
          val httpstatus_filesize = messageSplits(4).trim.split(" ")
          val httpstatus = httpstatus_filesize(0).trim
          val filesize = httpstatus_filesize(1).trim.toDouble
          val sendtime = messageSplits(9).trim.toDouble // 单位秒
          val reqstarttime = timestamp
          val reqendtime = (timestamp + sendtime * 1000).toLong // tranTimeToLong 返回的是毫秒 所以要乘1000
          val upstream_cache_status = messageSplits(11).trim
          val upstream_httpstatus = messageSplits(12).trim

          var res = 0.0
          if (sendtime != 0) {
            res = (filesize / 1024 / 1024) / (sendtime)
          }

          val upstream_filesize = 0.0 // 源数据中无，故全部置零
          val upres = 0.0

          var httpstatus_4xx = 0
          var httpststus_5xx = 0
          var upstream_4xx = 0
          var upstream_5xx = 0
          var hit_status = 0
          var hit_filesize = 0.0

          if (httpstatus.startsWith("5") && upstream_httpstatus.startsWith("5")) {
            httpststus_5xx = 1
          }
          if (httpstatus.startsWith("4") && upstream_httpstatus.startsWith("4")) {
            httpstatus_4xx = 1
          }
          if (upstream_httpstatus.startsWith("4")) {
            upstream_4xx = 1
          }
          if (upstream_httpstatus.startsWith("5")) {
            upstream_5xx = 1
          }
          if (upstream_cache_status.contains("HIT")) {
            hit_status = 1
            hit_filesize = filesize
          }
          val vendor = Constant.ZTE_VENDOR_NAME

          // 转换成自定义对象 => DF
          B2BModel(
            timestamp,
            city,
            server_ip,
            reqdomain,
            filesize,
            upstream_filesize,
            reqstarttime,
            reqendtime,
            res,
            upres,
            firsttime,
            vendor,
            httpstatus_4xx,
            httpststus_5xx,
            upstream_4xx,
            upstream_5xx,
            hit_status,
            1,
            hit_filesize
          )
        })
        val cal: Calendar = Calendar.getInstance()
        //调整时间
        cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
        val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
        val path = Constant.SPARK_STREAMING_WRITE_PATH + "/" + Constant.ZTE_KAFKA_B2B_TOPIC_NAME + "/" + dateStr

        zteRDD.coalesce(10).toDF().write.mode(SaveMode.Append).parquet(path)
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    //    conf.setMaster("local[4]")
  }
}
