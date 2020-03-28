package fun.lumia.kafkaToHdfs

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import fun.lumia.Constant
import fun.lumia.bean.scalaClass.B2BModel
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils.{tranTimeToLong, tranTimeb2bfh}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}


object fonsviewKafkaToHdfs extends SparkTool {

  override def run(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Durations.minutes(5))
    ssc.checkpoint(Constant.SPARK_STREAMING_CHECKPOINT_PATH + "/" + Constant.FONSVIEW_KAFKA_B2B_TOPIC_NAME)
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
      Set(Constant.FONSVIEW_KAFKA_B2B_TOPIC_NAME)
    ).map(_._2)

    ds.foreachRDD(fhSourceRDD => {
      if (!fhSourceRDD.isEmpty()) {
        val sqlContext = sql
        import sqlContext.implicits._
        // filter 不规则数据
        val fhRDD = fhSourceRDD.filter(line => {
          val json = JSON.parseObject(line)
          val message = json.getString("message")
          val messageSplits = message.split("\\|")
          val server_ip = messageSplits(1)
          val filesize = messageSplits(14)

          messageSplits.length == 46 &&
            !"".equals(messageSplits(0)) &&
            server_ip.contains(".") &&
            filesize.matches("[0-9]+")
        }).map(line => { // 取出需要的字段并转换成DataFrame，最后以parquet格式存储
          val json = JSON.parseObject(line)
          val tagsArray = json.getJSONArray("tags")
          var city = "未知城市"
          if (!tagsArray.isEmpty) {
            city = tagsArray.get(0).toString.trim()
          }
          val messageSplits = json.getString("message").split("\\|")
          val timestamp = tranTimeToLong(messageSplits(0))
          val server_ip = messageSplits(1)
          var reqdomain = messageSplits(6)
          if (reqdomain.contains(":")) {
            reqdomain = reqdomain.split(":")(0)
          }
          val filesize = messageSplits(14).toDouble
          var upstream_filesize = 0D
          if (messageSplits(31).matches("[0-9]+")) {
            upstream_filesize = messageSplits(31).toDouble
          }
          val reqstarttime = tranTimeb2bfh(messageSplits(15))
          val reqendtime = tranTimeb2bfh(messageSplits(16))
          var firsttime = 0.0
          if (!"null".equalsIgnoreCase(messageSplits(17))) {
            val responsetime = tranTimeb2bfh(messageSplits(17)).toDouble
            firsttime = (responsetime - reqstarttime) / 1000 // 单位换成秒
          }
          val httpstatus = messageSplits(11)
          val upstream_httpstatus = messageSplits(41)
          val reqhitstatus = messageSplits(12)
          // 速率
          var res = 0.0
          var upres = 0.0
          if (reqendtime - reqstarttime != 0) {
            val consistTime = reqendtime - reqstarttime
            res = (filesize / 1024 / 1024) / consistTime * 1000
            upres = (upstream_filesize / 1024 / 1024) / consistTime * 1000
          }
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
          if (reqhitstatus.contains("HIT")) {
            hit_status = 1
            hit_filesize = filesize
          }
          val vendor = Constant.FONSVIEW_VENDOR_NAME
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
        val path = Constant.SPARK_STREAMING_WRITE_PATH + "/" + Constant.FONSVIEW_KAFKA_B2B_TOPIC_NAME + "/" + dateStr

        fhRDD.coalesce(1).toDF().write.mode(SaveMode.Append).parquet(path)
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
