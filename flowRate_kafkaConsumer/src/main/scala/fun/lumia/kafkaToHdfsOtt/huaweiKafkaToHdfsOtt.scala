package fun.lumia.kafkaToHdfsOtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import fun.lumia.Constant
import fun.lumia.bean.scalaClass.OttModel
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils.tranTimeToLongOttHW
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}


object huaweiKafkaToHdfsOtt extends SparkTool {

  override def run(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Durations.minutes(5))
    //    val ssc = new StreamingContext(sc, Durations.seconds(5))

    // set checkpoint directory
    ssc.checkpoint(Constant.SPARK_STREAMING_CHECKPOINT_OTT_PATH + "/" + Constant.HUAWEI_VENDOR_NAME)

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Constant.KAFKA_BOOTSTRAP_SERVERS_OTT,
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

    val huaweiSourceDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(Constant.HUAWEI_KAFKA_OTT_TOPIC_NAME)
    ).map(_._2)
    huaweiSourceDS.print(10)


    // 解析huawei OTT  serverbyte/B  costtime/ms
    val huaweiDS = huaweiSourceDS.filter(line => {
      val json = JSON.parseObject(line)
      val messageSplit = json.getString("message").split("\\|")
      messageSplit.length == 17
    }).map(line => {
      val json = JSON.parseObject(line)
      val message = json.getString("message")
      val messageSplit = message.split("\\|")
      val timestamp = tranTimeToLongOttHW(messageSplit(1))

      val serverip = messageSplit(15).split("\\:")(0).trim

      val url = messageSplit(5)
      val urlsplit = url.split("\\/")
      var viewtype = ""
      if (url.contains("10086.cn")) {
        viewtype = updateViewtype(urlsplit(1))
      } else {
        viewtype = updateViewtype(urlsplit(0))
      }

      val vendor = Constant.HUAWEI_VENDOR_NAME

      val serverbyte = messageSplit(8).toDouble

      val costtime = messageSplit(3) toDouble

      var res = 0.0

      if (costtime != 0) {
        res = serverbyte / 1024 / 1024 / costtime * 1000
      }

      val endtime = timestamp
      val starttime = timestamp - costtime.toLong

      val firsttime = messageSplit(2).toDouble

      val httpstatus = messageSplit(7)
      val upstreamip_status = messageSplit(16)
      var httpstatus_2xx = 0
      var httpstatus_3xx = 0
      var httpstatus_4xx = 0
      var httpstatus_5xx = 0
      var upstatus_5xx = 0
      if (httpstatus.startsWith("5")) {
        httpstatus_5xx = 1
      }
      if (httpstatus.startsWith("4")) {
        httpstatus_4xx = 1
      }
      if (httpstatus.startsWith("3")) {
        httpstatus_3xx = 1
      }
      if (httpstatus.startsWith("2")) {
        httpstatus_2xx = 1
      }
      if (upstreamip_status.startsWith("5")) {
        upstatus_5xx = 1
      }
      OttModel(serverip,
        viewtype,
        vendor,
        res,
        starttime,
        endtime,
        firsttime,
        httpstatus_2xx,
        httpstatus_3xx,
        httpstatus_4xx,
        httpstatus_5xx,
        upstatus_5xx,
        1
      )
    })


    huaweiDS.foreachRDD(eachRDD => {
      if (!eachRDD.isEmpty()) {

        val cal: Calendar = Calendar.getInstance()

        //调整时间
        cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
        val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
        val path = Constant.SPARK_STREAMING_OTT_WRITE_PATH + "/" + Constant.HUAWEI_VENDOR_NAME + "/" + dateStr

        val sqlContext = sql
        import sqlContext.implicits._
        eachRDD.coalesce(10).toDF().write.mode(SaveMode.Append).parquet(path)
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

  def updateViewtype(viewtype: String): String = {
    var result = "其他"
    if (viewtype.equals("000000001000")) {
      result = "直播"
    } else if (viewtype.equals("000000000000")) {
      result = "点播"
    } else if (viewtype.equals("000000002000")) (result = "回看")
    result
  }

}
