package fun.lumia.utils

import fun.lumia.ConstantInCombine
import fun.lumia.bean.scalaClass.{B2BModel, OttModel}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object commonFun {
  def createKafkaSC(ssc: StreamingContext, topic: String): DStream[String] = {
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConstantInCombine.KAFKA_BOOTSTRAP_SERVERS,
      ConsumerConfig.GROUP_ID_CONFIG -> ConstantInCombine.GROUP_ID_CONFIG,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConstantInCombine.AUTO_OFFSET_RESET_CONFIG
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic)
    ).map(_._2)
  }

  def createKafkaSCOtt(ssc: StreamingContext, topic: String): DStream[String] = {
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConstantInCombine.KAFKA_BOOTSTRAP_SERVERS_OTT,
      ConsumerConfig.GROUP_ID_CONFIG -> ConstantInCombine.GROUP_ID_CONFIG,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConstantInCombine.AUTO_OFFSET_RESET_CONFIG
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic)
    ).map(_._2)
  }

  def createKafkaSCCpu(ssc: StreamingContext, topic: String): DStream[String] = {
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConstantInCombine.KAFKA_BOOTSTRAP_SERVERS_CPU,
      ConsumerConfig.GROUP_ID_CONFIG -> ConstantInCombine.GROUP_ID_CONFIG,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConstantInCombine.AUTO_OFFSET_RESET_CONFIG
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic)
    ).map(_._2)
  }

  def createEmptyB2BModelDF(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val emptyB2BModel = new B2BModel(
      timestamp = 0L,
      server_ip = "null",
      reqdomain = "null",
      filesize = 0,
      download_filesize = 0,
      upstream_filesize = 0,
      sendtime = 0,
      firsttime = 0,
      vendor = "null",
      httpstatus_4xx = 0,
      httpststus_5xx = 0,
      upstream_4xx = 0,
      upstream_5xx = 0,
      hit_status = 0,
      0,
      hit_filesize = 0
    )
    Seq(emptyB2BModel).toDF()
  }

  def createEmptyOttModelDF(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val emptyOttModel = new OttModel(
      server_ip = "null",
      viewtype = "null",
      vendor = "null",
      res = 0,
      starttime = 0,
      endtime = 0,
      firsttime = 0,
      httpstatus_2xx = 0,
      httpstatus_3xx = 0,
      httpstatus_4xx = 0,
      httpstatus_5xx = 0,
      upstatus_5xx = 0,
      total = 0
    )
    Seq(emptyOttModel).toDF()
  }
}
