package fun.lumia

import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils.tranTimeOttfh
import fun.lumia.utils.commonFun.createKafkaSCOtt
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

object getInvalidData extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    val duration = args(0).toInt
    val ssc = new StreamingContext(sc, Durations.minutes(duration))
    ssc.checkpoint(ConstantInCombine.SPARK_STREAMING_CHECKPOINT_OTT_PATH + "/invalidDataApp")

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConstantInCombine.KAFKA_BOOTSTRAP_SERVERS_OTT,
      ConsumerConfig.GROUP_ID_CONFIG -> "ustcGetInvalidData",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConstantInCombine.AUTO_OFFSET_RESET_CONFIG
    )
    val fonsviewDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(ConstantInCombine.FONSVIEW_KAFKA_OTT_TOPIC_NAME)
    ).map(_._2)
    val fonsview = fonsviewDS.mapPartitions(rdd => {
      rdd.filter(line => {
        val JsonObject = JSON.parseObject(line)
        val messageSplit = JsonObject.getString("message").split("\\|")
        val reqstarttime = tranTimeOttfh(messageSplit(13))
        val reqendtime = tranTimeOttfh(messageSplit(14))
        reqendtime < reqstarttime || messageSplit.length != 57
      }).map(line => {
        sourceLine(line)
      })
    })

    val sqlContext = sql
    import sqlContext.implicits._
    fonsview.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).toDF().write.mode(SaveMode.Append).text("/user/lumia/invalidData/fonsview/")
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
    conf.setAppName("getInvalidData")
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "10000000")
    conf.set("spark.sql.shuffle.partitions", "10")
    conf.set("spark.shuffle.file.buffer", "64k")
    conf.set("spark.reducer.maxSizeInFlight", "64m")
    conf.set("spark.cleaner.ttl", "172800")
  }

  case class sourceLine(json: String)

}
