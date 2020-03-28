package fun.lumia.elasticSearch

import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import fun.lumia.ConstantInCombine
import fun.lumia.bean.scalaClass.CpuPuluo
import fun.lumia.common.SparkTool
import fun.lumia.utils.commonFun.createKafkaSCCpu
import fun.lumia.utils.DateUtils.tranTimeWithPu
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.elasticsearch.spark.sql._

object cpuToES extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    val duration = args(0).toInt
    val ssc = new StreamingContext(sc, Durations.minutes(duration))
    ssc.checkpoint(ConstantInCombine.SPARK_STREAMING_CHECKPOINT_PATH + "/" + ConstantInCombine.CPU_CHECKPOINT_PATH_NAME)

    val cpuDS = createKafkaSCCpu(ssc, ConstantInCombine.CPU_KAFKA_TOPIC_NAME)
    val sqlContext = sql
    import sqlContext.implicits._
    cpuDS.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val cpuRDD = rdd.coalesce(100).filter(line => {
          val json = JSON.parseObject(line)
          val kpi_type = json.getString("name")
          val value = json.getString("value")
          val labels = JSON.parseObject(json.getString("labels"))
          val server = labels.getString("instance")
          kpi_type.equals("node_cpu_seconds_total") &&
            server.contains("-") &&
            Pattern.matches("\\d+\\.*\\d*", value) //&& value.matches("[0-9]+")
        }).map(line => {
          val json = JSON.parseObject(line)
          //          val kpi_type = json.getString("name")
          val time_stamp = json.getString("timestamp")
          val value = json.getString("value").toDouble
          val labels = JSON.parseObject(json.getString("labels"))
          val busi_name = labels.getString("business")
          val dc = labels.getString("dc")
          val group = labels.getString("group")
          val instance = labels.getString("instance")
          val server_ip = instance.split("-")(1)
          val cpu = labels.getString("cpu")
          val vendor = labels.getString("vendor")
          val mode = labels.getString("mode")
          CpuPuluo(tranTimeWithPu(time_stamp),
            tranTimeWithPu(time_stamp),
            instance,
            dc,
            busi_name,
            group,
            server_ip,
            vendor,
            mode,
            cpu,
            value)
        })
        cpuRDD.toDF().saveToEs(ConstantInCombine.CPU_ES_INDEX)
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
    // Elastic Search 配置参数
    conf.set("cluster.name", "fh_topic")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "112.29.140.223:9200")
    conf.set("es.port", "9200")
    conf.set("es.index.read.missing.as.empty", "true")
    conf.set("es.nodes.wan.only", "true")

    // spark 参数设置
    conf.setAppName("cpuToES")
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "10000000")
    conf.set("spark.sql.shuffle.partitions", "100")
    conf.set("spark.shuffle.file.buffer", "64k")
    conf.set("spark.reducer.maxSizeInFlight", "64m")
    conf.set("spark.cleaner.ttl", "172800")
  }
}
