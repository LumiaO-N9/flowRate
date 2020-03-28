package fun.lumia.kafkaToHdfsOtt

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import fun.lumia.Constant
import fun.lumia.bean.scalaClass.OttModel
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils.{isNullOrEmptyAfterTrim, tranTimeOttFh, tranTimeToLong, tranTimeWithhy, tranTimeb2bhy}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}


object fonsviewKafkaToHdfsOtt extends SparkTool {

  override def run(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Durations.minutes(5))
    //    val ssc = new StreamingContext(sc, Durations.seconds(5))

    // set checkpoint directory
    ssc.checkpoint(Constant.SPARK_STREAMING_CHECKPOINT_OTT_PATH + "/" + Constant.FONSVIEW_VENDOR_NAME)

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
    val hdpSourceDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(Constant.FONSVIEW_KAFKA_OTT_HDP_TOPIC_NAME)
    ).map(_._2)


    val hlsSourceDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(Constant.FONSVIEW_KAFKA_OTT_HLS_TOPIC_NAME)
    ).map(_._2)


    val errorSourceDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(Constant.FONSVIEW_KAFKA_OTT_ERROR_TOPIC_NAME)
    ).map(_._2)

    // 解析烽火hdp  serverbyte/KB  costtime/s other Byte/ms
    val hdpDS = hdpSourceDS.filter(line => {
      val json = JSON.parseObject(line)
      val messageSplit = json.getString("message").split("\\|")
      messageSplit.length == 23
    }).map(line => {
      val json = JSON.parseObject(line)
      val message = json.getString("message")
      val messageSplit = message.split("\\|")
      val timestamp = tranTimeOttFh(messageSplit(10))
      val serverip = json.getString("serverip")

      val viewtype = updateViewtypefh(messageSplit(6))
      val vendor = Constant.FONSVIEW_VENDOR_NAME

      val serverbyte = getServerbyte(messageSplit(12)) // 单位转换成B
      val costtime = getCostTime(messageSplit(11)) // 单位换成ms

      var res = 0.0
      if (costtime != 0) {
        res = serverbyte / 1024 / 1024 / costtime * 1000
      }
      // timestamp = endtime
      // starttime = endtime-costtime  单位都是ms
      val endtime = timestamp
      val starttime = timestamp - costtime.toLong
      val firsttimeStr = messageSplit(14) // 单位同costtime 原单位是s
      var firsttime = 0.0 // 单位同costtime 原单位是s
      if (!isNullOrEmptyAfterTrim(firsttimeStr)) {
        firsttime = firsttimeStr.toDouble
      }

      val httpstatus_2xx = 1
      val httpstatus_3xx = 0
      val httpstatus_4xx = 0
      val httpstatus_5xx = 0
      val upstatus_5xx = 0
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

    // 解析烽火hls  serverbyte/KB  costtime/s other Byte/ms
    val hlsDS = hlsSourceDS.filter(line => {
      val json = JSON.parseObject(line)
      val messageSplit = json.getString("message").split("\\|")
      messageSplit.length == 38
    }).map(line => {
      val json = JSON.parseObject(line)
      val message = json.getString("message")
      val messageSplit = message.split("\\|")
      val timestamp = tranTimeOttFh(messageSplit(8))
      val serverip = json.getString("serverip")
      val viewtype = updateViewtypefh(messageSplit(6))

      val vendor = Constant.FONSVIEW_VENDOR_NAME

      val serverbyte = getServerbyte(messageSplit(12)) // 单位换成B
      val costtime = getCostTime(messageSplit(11)) // 单位换成ms

      var res = 0.0
      if (costtime != 0) {
        res = serverbyte / 1024 / 1024 / costtime * 1000
      }
      val endtime = timestamp
      val starttime = timestamp - costtime.toLong
      val firsttimeStr = messageSplit(16) // 单位同costtime 原单位是s
      var firsttime = 0.0 // 单位同costtime 原单位是s
      if (!isNullOrEmptyAfterTrim(firsttimeStr)) {
        firsttime = firsttimeStr.toDouble
      }


      val httpstatus_2xx = 1
      val httpstatus_3xx = 0
      val httpstatus_4xx = 0
      val httpstatus_5xx = 0
      val upstatus_5xx = 0
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

    // 解析烽火error  serverbyte/KB  costtime/s other Byte/ms
    val errorDS = errorSourceDS.filter(line => {
      val json = JSON.parseObject(line)
      val messageSplit = json.getString("message").split("\\|")
      messageSplit.length == 15
    }).map(line => {
      val json = JSON.parseObject(line)
      val message = json.getString("message")
      val messageSplit = message.split("\\|")
      val timestamp = tranTimeOttFh(messageSplit(10))
      val serverip = messageSplit(9)

      val viewtype = updateViewtypefh(messageSplit(0))
      val vendor = Constant.FONSVIEW_VENDOR_NAME

      val res = 0.0
      // timestamp = endtime
      // starttime = endtime-costtime  单位都是ms
      val endtime = timestamp
      val starttime = timestamp
      val firsttime = 0.0

      val httpstatus_2xx = 0
      val httpstatus_3xx = 0
      val httpstatus_4xx = 0
      val httpstatus_5xx = 1
      val upstatus_5xx = 0

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

    val unionDS = hdpDS.union(hlsDS).union(errorDS)


    unionDS.foreachRDD(eachRDD => {
      if (!eachRDD.isEmpty()) {
        val cal: Calendar = Calendar.getInstance()
        //调整时间
        cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
        val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
        val path = Constant.SPARK_STREAMING_OTT_WRITE_PATH + "/" + Constant.FONSVIEW_VENDOR_NAME + "/" + dateStr
        val sqlContext = sql
        import sqlContext.implicits._
        eachRDD.coalesce(5).toDF().write.mode(SaveMode.Append).parquet(path)
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

  def updateViewtypefh(viewtype: String): String = {
    var result = "其他"
    if (viewtype.equals("01")) {
      result = "直播"
    } else if (viewtype.equals("02")) {
      result = "回看"
    } else if (viewtype.equals("00")) (result = "点播")
    result
  }

  def getServerbyte(serverbyte: String): Double = {
    var result = 0.0
    if (!isNullOrEmptyAfterTrim(serverbyte)) {
      result = serverbyte.toDouble * 1024
    }
    result
  }

  def getCostTime(costtime: String): Double = {
    var result = 0.0
    if (!isNullOrEmptyAfterTrim(costtime)) {
      result = costtime.toDouble * 1000
    }
    result
  }
}
