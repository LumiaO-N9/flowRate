package fun.lumia.util

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import fun.lumia.bean.scalaClass.{B2BModel, OttModel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object loadDFfunctions {
  def createEmptyB2BModelDF(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val emptyB2BModel = new B2BModel(
      timestamp = 0L,
      city = "null",
      server_ip = "null",
      reqdomain = "null",
      filesize = 0,
      upstream_filesize = 0,
      reqstarttime = 0,
      reqendtime = 0,
      res = 0,
      upres = 0,
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

  def ifPathExists(path: String, sparkContext: SparkContext): Boolean = {
    val hadoopConf: Configuration = sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    fs.exists(new Path(path))
  }


  def getJudgeTime(): Long = {
    val currentTime = new Date().getTime()
    val delayMinute: Int = 30 // minutes
    val secondFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    // 只取当前时间的秒数 忽略毫秒 的 timestamp
    val currentTimeSecond = secondFormat.parse(secondFormat.format(currentTime)).getTime()
    val judgeTime = currentTimeSecond - delayMinute * 60 * 1000
    judgeTime
  }

  //  def constructPathStrByTime(time: Long) = {
  //    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
  //    "/" + dateFormat.format(time)
  //  }

  def constructPath(frontPath: String, vendor: String, time: Long): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    val dfPath = frontPath + "/" + vendor + "/" + dateFormat.format(time)
    dfPath
  }

  def loadDFByPath(path: String, sparkContext: SparkContext, sqlContext: SQLContext): DataFrame = {
    var someDF = createEmptyOttModelDF(sqlContext)
    if (Pattern.matches("^.*b2b.*$", path)) {
      someDF = createEmptyB2BModelDF(sqlContext)
    }
    if (ifPathExists(path, sparkContext)) {
      someDF = sqlContext.read.parquet(path)
    }
    someDF
  }

  def loadUnionDFByVendor(judgeTime: Long, frontPath: String, vendor: String, sparkContext: SparkContext, sqlContext: SQLContext): DataFrame = {
    // 根据区间起始时间戳获取前一个小时时间时间戳(毫秒)
    val frontOneHourTime = judgeTime - 3600 * 1000
    val frontTwoHourTime = frontOneHourTime - 3600 * 1000
    val currentHourPath = constructPath(frontPath, vendor, judgeTime)
    val frontOneHourPath = constructPath(frontPath, vendor, frontOneHourTime)
    //    val frontTwoHourPath = constructPath(frontPath, vendor, frontTwoHourTime)

    val currentDF = loadDFByPath(currentHourPath, sparkContext, sqlContext)
    val frontOneHourDF = loadDFByPath(frontOneHourPath, sparkContext, sqlContext)
    //    val frontTwoHourDF = loadDFByPath(frontTwoHourPath, sparkContext, sqlContext)
    currentDF
      .unionAll(frontOneHourDF)
    //      .unionAll(frontTwoHourDF)
  }
}
