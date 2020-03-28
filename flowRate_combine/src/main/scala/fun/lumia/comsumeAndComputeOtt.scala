package fun.lumia

import java.sql.DriverManager
import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import fun.lumia.bean.scalaClass.{B2BModel, OttModel, unionOTT}
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils._
import fun.lumia.utils.commonFun.createKafkaSCOtt
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ListBuffer

object comsumeAndComputeOtt extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    val duration = args(0).toInt
    val ssc = new StreamingContext(sc, Durations.minutes(duration))
    ssc.checkpoint(ConstantInCombine.SPARK_STREAMING_CHECKPOINT_PATH + "/ott" + ConstantInCombine.COMBINE_CHECKPOINT_PATH_NAME)

    val fonsviewDS = createKafkaSCOtt(ssc, ConstantInCombine.FONSVIEW_KAFKA_OTT_TOPIC_NAME)

    val huaweiDS = createKafkaSCOtt(ssc, ConstantInCombine.HUAWEI_KAFKA_OTT_TOPIC_NAME)

    val zteDS = createKafkaSCOtt(ssc, ConstantInCombine.ZTE_KAFKA_OTT_TOPIC_NAME_NEW)
    val fonsview = fonsviewDS.mapPartitions(rdd => {
      rdd.filter(line => {
        val JsonObject = JSON.parseObject(line)
        val messageSplit = JsonObject.getString("message").split("\\|")
        val server_ip = messageSplit(21)
        val reqstarttime = tranTimeOttfh(messageSplit(13))
        val reqendtime = tranTimeOttfh(messageSplit(14))
        messageSplit.length == 57 && reqendtime >= reqstarttime && server_ip.contains(".")
      }).flatMap(line => {
        val JsonObject = JSON.parseObject(line)
        val message = JsonObject.getString("message").split("\\|")
        //时间戳
        val viewtype = JudgeViewTypeFH(message(7))
        val server_ip = message(21)
        val reqstarttime = tranTimeOttfh(message(13))
        val reqendtime = tranTimeOttfh(message(14))
        val sendtime = reqendtime - reqstarttime
        val filesize = message(25).toDouble
        var upstream_filesize = 0.0
        if (!message(51).equalsIgnoreCase("null")) {
          upstream_filesize = message(51).toDouble
        }
        val httpstatus = message(35)
        //        val cacheStatus = message(36)
        val upstream_httpstatus = "0"
        val firsttime = 0
        val tscount = 0
        val tsbadcount = 0
        val vendor = ConstantInCombine.FONSVIEW_VENDOR_SIMPLE_NAME
        splitLineDataOtt(server_ip, viewtype, filesize, upstream_filesize,
          reqstarttime, reqendtime, sendtime, firsttime, vendor, httpstatus,
          upstream_httpstatus, tscount, tsbadcount)
      })
    })
    val huawei = huaweiDS.mapPartitions(rdd => {
      rdd.filter(line => {
        val jsonObject = JSON.parseObject(line)
        val message = jsonObject.getString("message").split("\\|")
        val reqstarttime = message(23)
        //请求结束时间
        val reqendtime = message(24)
        message.length >= 37 && reqendtime >= reqstarttime && Pattern.matches(".*[a-zA-Z]+.*", reqstarttime) && //判断是否带字母过滤脏数据
          Pattern.matches(".*[a-zA-Z]+.*", reqendtime)
      }).flatMap(line => {
        val jsonObject = JSON.parseObject(line)
        val message = jsonObject.getString("message").split("\\|")
        //时间戳  20191206T040941Z+08
        //        val timestamp = tranTimeOttHW(message(1))
        //ip  111.38.130.25:80
        val server_ip = message(4).split(":")(0)
        //观看类型
        val viewtype = JudgeViewTypeHW(message(13))
        //filesize
        val filesize = message(20).toDouble
        //请求开始时间
        val reqstarttime = tranTimeOttHW(message(23))
        //请求结束时间
        val reqendtime = tranTimeOttHW(message(24))
        //状态码
        val httpstatus = message(26)
        //回源filesize
        val upstream_filesize = message(28).toDouble
        //首包时延
        val firsttime = message(31).toDouble
        //数据传输时长
        val sendtime = reqendtime - reqstarttime

        //        val cacheStatus = "0"
        val upstream_httpstatus = "0"
        val tscount = 0
        val tsbadcount = 0
        val vendor = ConstantInCombine.HUAWEI_VENDOR_SIMPLE_NAME
        splitLineDataOtt(server_ip, viewtype, filesize, upstream_filesize,
          reqstarttime, reqendtime, sendtime, firsttime, vendor, httpstatus,
          upstream_httpstatus, tscount, tsbadcount)
      })
    })

    //    val zte = zteDS.mapPartitions(rdd => {
    //      rdd.filter(line => {
    //        val json = JSON.parseObject(line)
    //        val message = json.getString("message")
    //        val messageSplit = message.split("\\|")
    //        var judgemessage = false
    //        var timestamp_judge = false
    //        if (messageSplit.length > 0) {
    //          judgemessage = true
    //        }
    //        if (judgemessage) {
    //          val timestamp = messageSplit(7)
    //          timestamp_judge = timestamp.contains(":")
    //        }
    //        judgemessage && timestamp_judge
    //      })
    //        .flatMap(line => {
    //          val json = JSON.parseObject(line)
    //          //          val city = json.getJSONArray("tags").get(0).toString.trim
    //          //          val ip = json.getJSONArray("tags").get(1).toString.trim
    //          val message = json.getString("message")
    //          val messageSplit = message.split("\\|")
    //          val server_ip = messageSplit(2)
    //          //          val timestamp = tranTimeToLong(tranTimeOttZte(messageSplit(7)))
    //          val reqstarttime = tranTimeOttzte(messageSplit(8))
    //          val reqendtime = tranTimeOttzte(messageSplit(9))
    //          val sendtime = reqendtime - reqstarttime
    //          val filesize = messageSplit(14).toDouble
    //          val viewtype = updateViewtypenodepad(messageSplit(23))
    //          val firsttime = messageSplit(32).toDouble
    //          val httpstatus = messageSplit(37)
    //          //          val cacheStatus = messageSplit(42)
    //          val upstream_httpstatus = messageSplit(46)
    //          //不确定，没值
    //          val upstream_filesize = messageSplit(50).toDouble
    //          val tscount = 0
    //          val tsbadcount = 0
    //          val vendor = ConstantInCombine.ZTE_VENDOR_SIMPLE_NAME
    //          splitLineDataOtt(server_ip, viewtype, filesize, upstream_filesize,
    //            reqstarttime, reqendtime, sendtime, firsttime, vendor, httpstatus,
    //            upstream_httpstatus, tscount, tsbadcount)
    //        })
    //    })
    val zte = zteDS.mapPartitions(rdd => {
      rdd.filter(line => {
        val json = JSON.parseObject(line)
        val message = json.getString("message")
        val messagesplit = message.split("\\|")
        val reqstarttime = messagesplit(8).trim
        val reqendtime = messagesplit(9).trim
        val regexStr = "\\d{8}\\s{1}\\d{2}:\\d{2}:\\d{2}\\.\\d{3}"
        messagesplit.length >= 51 && reqendtime >= reqstarttime && Pattern.matches(regexStr, reqstarttime) && Pattern.matches(regexStr, reqendtime)
      })
        .flatMap(line => {
          val json = JSON.parseObject(line)
          val message = json.getString("message")

          val messagesplit = message.split("\\|")
          val server_ip = messagesplit(2)
          //          val timestamp = tranTimeToLong(tranTimeOttZte(messagesplit(7)))
          val reqstarttime = tranTimeOttzte(messagesplit(8))
          val reqendtime = tranTimeOttzte(messagesplit(9))
          val sendtime = reqendtime - reqstarttime
          val viewtype = JudgeViewTypeZTE(messagesplit(23))
          //自己根据数据查看的。配置文件不对
          //          val responsetime = messagesplit(43)
          //根据配置文件来得
          var firsttime = 0.0
          var filesize = 0.0
          var upstream_filesize = 0.0
          val regexStr = "\\d+\\.*\\d*"
          if (Pattern.matches(regexStr, messagesplit(32))) {
            firsttime = messagesplit(32).toDouble //ms
          }
          if (Pattern.matches(regexStr, messagesplit(14))) {
            filesize = messagesplit(14).toDouble
          }
          if (Pattern.matches(regexStr, messagesplit(50))) {
            upstream_filesize = messagesplit(50).toDouble
          }
          val httpstatus = messagesplit(37)
          val upstream_httpstatus = messagesplit(46).trim
          val tscount = 0
          val tsbadcount = 0
          val vendor = ConstantInCombine.ZTE_VENDOR_SIMPLE_NAME
          splitLineDataOtt(server_ip, viewtype, filesize, upstream_filesize,
            reqstarttime, reqendtime, sendtime, firsttime, vendor, httpstatus,
            upstream_httpstatus, tscount, tsbadcount)
        })
    })
    val unionSS = fonsview.union(huawei).union(zte)
    //    print("*" * 200)
    //    unionSS.print(10)
    //    print("*" * 200)
    val sqlContext = sql
    import sqlContext.implicits._
    sqlContext.udf.register("formatTimeTemp", formatTimeTemp)
    unionSS.foreachRDD(rdd => {
      val unionDF = rdd.toDF().coalesce(100)
      unionDF.registerTempTable("UnionDFTable")
      val finalDF = sql.sql(
        """
          |select timestamp as time_stamp,
          |server_ip,
          |vendor,
          |viewtype,
          |formatTimeTemp(timestamp) as format_timestamp,
          |sum(http_4xx_count) as count_4xx,
          |sum(http_5xx_count) as count_5xx,
          |sum(http_200_count) as count_200,
          |sum(http_206_count) as count_206,
          |sum(http_301_count) as count_301,
          |sum(http_302_count) as count_302,
          |sum(http_403_count) as count_403,
          |sum(http_404_count) as count_404,
          |sum(http_502_count) as count_502,
          |sum(http_503_count) as count_503,
          |sum(filesize) as sum_filesize,
          |sum(download_filesize) as sum_download_filesize,
          |sum(tscount) as sum_tscount,
          |sum(tsbadcount) as sum_tsbadcount,
          |sum(total) as total,
          |sum(firsttime) as sum_firsttime,
          |sum(upstream_filesize) as sum_upstream_filesize,
          |sum(sendtime) as sum_sendtime
          |from UnionDFTable group by timestamp,server_ip,vendor,viewtype
          |""".stripMargin)
      finalDF.cache()
      val prop = new Properties()
      prop.put("driver", ConstantInCombine.MYSQL_DRIVER)
      prop.put("user", ConstantInCombine.MYSQL_USER)
      prop.put("password", ConstantInCombine.MYSQL_PASSWORD)
      //      finalDF.show(10)
      finalDF.write.mode(SaveMode.Overwrite).jdbc(ConstantInCombine.MYSQL_URL, ConstantInCombine.MYSQL_DATABLE_OTT_KPI_DWS_LUMIA, prop)
      finalDF.foreachPartition(iter => {
        val conn = DriverManager.getConnection(ConstantInCombine.MYSQL_URL, ConstantInCombine.MYSQL_USER, ConstantInCombine.MYSQL_PASSWORD)
        conn.setAutoCommit(false)
        val dbTable = ConstantInCombine.MYSQL_DATABLE_OTT_KPI_LUMIA
        val sql =
          s"""
             |INSERT INTO $dbTable (time_stamp,server_ip,vendor,
             |viewtype,format_timestamp,count_4xx,count_5xx,
             |count_200,count_206,count_301,count_302,
             |count_403,count_404,count_502,count_503,
             |sum_filesize,sum_tscount,sum_tsbadcount,
             |total,sum_firsttime,sum_upstream_filesize,sum_sendtime,sum_download_filesize)
             |VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
             |ON DUPLICATE KEY UPDATE sum_filesize=sum_filesize+?,
             |count_4xx=count_4xx+?,
             |count_5xx=count_5xx+?,
             |count_200=count_200+?,
             |count_206=count_206+?,
             |count_301=count_301+?,
             |count_302=count_302+?,
             |count_403=count_403+?,
             |count_404=count_404+?,
             |count_502=count_502+?,
             |count_503=count_503+?,
             |sum_tscount=sum_tscount+?,
             |sum_tsbadcount=sum_tsbadcount+?,
             |total=total+?,
             |sum_firsttime=sum_firsttime+?,
             |sum_upstream_filesize=sum_upstream_filesize+?,
             |sum_sendtime=sum_sendtime+?,
             |sum_download_filesize=sum_download_filesize+?
             |""".stripMargin
        //          val sql = s"insert into $dbTable (time_stamp,server_ip,total) values(?,?,?) ON DUPLICATE KEY UPDATE total=total+?"

        val stat = conn.prepareStatement(sql)

        iter.foreach(row => {

          val time_stamp = row.getAs[Long]("time_stamp")
          val server_ip = row.getAs[String]("server_ip")
          val vendor = row.getAs[String]("vendor")
          val viewtype = row.getAs[String]("viewtype")
          val format_timestamp = row.getAs[String]("format_timestamp")
          val count_4xx = row.getAs[Long]("count_4xx")
          val count_5xx = row.getAs[Long]("count_5xx")
          val count_200 = row.getAs[Long]("count_200")
          val count_206 = row.getAs[Long]("count_206")
          val count_301 = row.getAs[Long]("count_301")
          val count_302 = row.getAs[Long]("count_302")
          val count_403 = row.getAs[Long]("count_403")
          val count_404 = row.getAs[Long]("count_404")
          val count_502 = row.getAs[Long]("count_502")
          val count_503 = row.getAs[Long]("count_503")
          val sum_filesize = row.getAs[Double]("sum_filesize")
          val sum_download_filesize = row.getAs[Double]("sum_download_filesize")
          val sum_tscount = row.getAs[Long]("sum_tscount")
          val sum_tsbadcount = row.getAs[Long]("sum_tsbadcount")
          val total = row.getAs[Long]("total")
          val sum_firsttime = row.getAs[Double]("sum_firsttime")
          val sum_upstream_filesize = row.getAs[Double]("sum_upstream_filesize")
          val sum_sendtime = row.getAs[Long]("sum_sendtime")
          //        println(time_stamp)
          //        println(time_flag)
          //        println(server_ip)
          stat.setLong(1, time_stamp)
          stat.setString(2, server_ip)
          stat.setString(3, vendor)
          stat.setString(4, viewtype)
          stat.setString(5, format_timestamp)
          stat.setLong(6, count_4xx)
          stat.setLong(7, count_5xx)
          stat.setLong(8, count_200)
          stat.setLong(9, count_206)
          stat.setLong(10, count_301)
          stat.setLong(11, count_302)
          stat.setLong(12, count_403)
          stat.setLong(13, count_404)
          stat.setLong(14, count_502)
          stat.setLong(15, count_503)
          stat.setDouble(16, sum_filesize)
          stat.setLong(17, sum_tscount)
          stat.setLong(18, sum_tsbadcount)
          stat.setLong(19, total)
          stat.setDouble(20, sum_firsttime)
          stat.setDouble(21, sum_upstream_filesize)
          stat.setLong(22, sum_sendtime)
          stat.setDouble(23, sum_download_filesize)
          stat.setDouble(24, sum_filesize)
          stat.setLong(25, count_4xx)
          stat.setLong(26, count_5xx)
          stat.setLong(27, count_200)
          stat.setLong(28, count_206)
          stat.setLong(29, count_301)
          stat.setLong(30, count_302)
          stat.setLong(31, count_403)
          stat.setLong(32, count_404)
          stat.setLong(33, count_502)
          stat.setLong(34, count_503)
          stat.setLong(35, sum_tscount)
          stat.setLong(36, sum_tsbadcount)
          stat.setLong(37, total)
          stat.setDouble(38, sum_firsttime)
          stat.setDouble(39, sum_upstream_filesize)
          stat.setLong(40, sum_sendtime)
          stat.setDouble(41, sum_download_filesize)


          stat.addBatch()

        })
        stat.executeBatch()
        conn.commit()
        stat.close()
        conn.close()
      })
      finalDF.unpersist()
    }

    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    conf.setAppName("comsumeAndComputeOtt")
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "10000000")
    conf.set("spark.sql.shuffle.partitions", "100")
    conf.set("spark.shuffle.file.buffer", "64k")
    conf.set("spark.reducer.maxSizeInFlight", "64m")
    conf.set("spark.cleaner.ttl", "172800")

  }

  def calcuNewFilesize(consistTime: Long, filesize: Double, sendtime: Long): Double = {
    consistTime * filesize / sendtime
  }

  def handleStatusOtt(httpstatus: String, upstream_httpstatus: String): List[Int] = {
    var http_200_count = 0
    var http_206_count = 0
    var http_301_count = 0
    var http_302_count = 0
    var http_403_count = 0
    var http_404_count = 0
    var http_4xx_count = 0
    var http_502_count = 0
    var http_503_count = 0
    var http_5xx_count = 0
    var upstream_5xx_count = 0

    if (httpstatus.equalsIgnoreCase("200")) {
      http_200_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("206")) {
      http_206_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("301")) {
      http_301_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("302")) {
      http_302_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("403")) {
      http_403_count = 1
      http_4xx_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("404")) {
      http_404_count = 1
      http_4xx_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("502")) {
      http_502_count = 1
      http_5xx_count = 1
    }
    else if (httpstatus.equalsIgnoreCase("503")) {
      http_503_count = 1
      http_5xx_count = 1
    }
    if (upstream_httpstatus.startsWith("5")) {
      upstream_5xx_count = 1
    }
    List[Int](http_200_count, http_206_count, http_301_count,
      http_302_count, http_403_count, http_404_count, http_4xx_count,
      http_502_count, http_503_count, http_5xx_count, upstream_5xx_count)
  }

  def splitLineDataOtt(server_ip: String, viewtype: String, filesize: Double, upstream_filesize: Double,
                       reqstarttime: Long, reqendtime: Long, sendtime: Long, firsttime: Double,
                       vendor: String, httpstatus: String, upstream_httpstatus: String,
                       tscount: Int, tsbadcount: Int): ListBuffer[unionOTT] = {

    val statusList = handleStatusOtt(httpstatus, upstream_httpstatus)
    val http_200_count = statusList(0)
    val http_206_count = statusList(1)
    val http_301_count = statusList(2)
    val http_302_count = statusList(3)
    val http_403_count = statusList(4)
    val http_404_count = statusList(5)
    val http_4xx_count = statusList(6)
    val http_502_count = statusList(7)
    val http_503_count = statusList(8)
    val http_5xx_count = statusList(9)
    val upstream_5xx_count = statusList(10)

    val StartTimeFormat = formatFhStartLong(reqstarttime)
    val EndTimeFormat = formatFhEndLong(reqendtime)
    var i = 1
    var timestampTemp = StartTimeFormat
    val windowGap = 5 * 60 * 1000
    val count = (EndTimeFormat - StartTimeFormat) / windowGap
    val list = new ListBuffer[unionOTT]
    var filesizeNew: Double = filesize
    var download_filesizeNew: Double = filesize
    var upstream_filesizeNew: Double = upstream_filesize
    var consistTime = 0L
    if (sendtime != 0) {
      if (count > 1) {
        while (i < count) {
          filesizeNew = 0.0
          download_filesizeNew = 0.0
          upstream_filesizeNew = 0.0
          var consistTimeTemp = 0L
          if (i == 1) {
            consistTimeTemp = timestampTemp + windowGap - reqstarttime

          } else {
            consistTimeTemp = windowGap
          }
          filesizeNew = calcuNewFilesize(consistTimeTemp, filesize, sendtime)
          download_filesizeNew = filesizeNew
          upstream_filesizeNew = calcuNewFilesize(consistTimeTemp, upstream_filesize, sendtime)
          list.append(new unionOTT(timestampTemp, server_ip, vendor, viewtype,
            http_4xx_count, http_5xx_count, http_200_count, http_206_count,
            http_301_count, http_302_count, http_403_count, http_404_count,
            http_502_count, http_503_count, upstream_5xx_count, filesizeNew, download_filesizeNew, tscount, tsbadcount,
            1, firsttime, upstream_filesizeNew, sendtime
          ))
          timestampTemp = timestampTemp + windowGap
          i += 1
        }
        consistTime = reqendtime - timestampTemp
      } else {
        consistTime = reqendtime - reqstarttime
      }
      filesizeNew = calcuNewFilesize(consistTime, filesize, sendtime)
      download_filesizeNew = filesizeNew
      upstream_filesizeNew = calcuNewFilesize(consistTime, upstream_filesize, sendtime)
    } else {
      filesizeNew = filesize
      download_filesizeNew = 0
      upstream_filesizeNew = upstream_filesize
    }

    list.append(new unionOTT(timestampTemp, server_ip, vendor, viewtype,
      http_4xx_count, http_5xx_count, http_200_count, http_206_count,
      http_301_count, http_302_count, http_403_count, http_404_count,
      http_502_count, http_503_count, upstream_5xx_count, filesizeNew, download_filesizeNew, tscount, tsbadcount,
      1, firsttime, upstream_filesizeNew, sendtime
    ))
    list
  }

  def JudgeViewTypeFH(viewtype: String): String = {
    var res = "其他"
    if (viewtype.equalsIgnoreCase("00")) {
      res = "点播"
    }
    if (viewtype.equalsIgnoreCase("01") || viewtype.equalsIgnoreCase("04")) {
      res = "直播"
    }
    if (viewtype.equalsIgnoreCase("02")) {
      res = "回看"

    }
    if (viewtype.equalsIgnoreCase("03") || viewtype.equalsIgnoreCase("05")) {
      res = "时移"

    }
    res
  }

  def JudgeViewTypeHW(viewtype: String): String = {
    var res = "其他"
    if (viewtype.equalsIgnoreCase("200")) {
      res = "点播"
    }
    if (viewtype.equalsIgnoreCase("201")) {
      res = "直播"
    }
    if (viewtype.equalsIgnoreCase("203")) {
      res = "回看"

    }
    if (viewtype.equalsIgnoreCase("202")) {
      res = "时移"

    }
    res
  }

  def updateViewtypenodepad(viewtype: String): String = {
    var result = "其他"
    if (viewtype.equals("000000001000")) {
      result = "直播"
    } else if (viewtype.equals("000000000000")) {
      result = "点播"
    } else if (viewtype.equals("000000002000")) (result = "回看")
    result
  }

  def JudgeViewTypeZTE(viewtype: String): String = {
    var res = "其他"
    if (viewtype.equalsIgnoreCase("1")) {
      res = "点播"
    }
    if (viewtype.equalsIgnoreCase("2")) {
      res = "直播"
    }
    if (viewtype.equalsIgnoreCase("3")) {
      res = "时移"
    }
    if (viewtype.equalsIgnoreCase("4")) {
      res = "回看"
    }
    if (viewtype.equalsIgnoreCase("5")) {
      res = "轮播"
    }
    if (viewtype.equalsIgnoreCase("6")) {
      res = "网络个人录制"
    }
    res
  }
}
