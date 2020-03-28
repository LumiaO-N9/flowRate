package fun.lumia

import java.sql.DriverManager
import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import fun.lumia.bean.scalaClass.B2BModel
import fun.lumia.common.SparkTool
import fun.lumia.utils.DateUtils._
import fun.lumia.utils.commonFun.createKafkaSC
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ListBuffer

object comsumeAndComputeTest extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    def createKafkaSCTest(ssc: StreamingContext, topic: String): DStream[String] = {
      val kafkaParams = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConstantInCombine.KAFKA_BOOTSTRAP_SERVERS,
        ConsumerConfig.GROUP_ID_CONFIG -> ConstantInCombine.GROUP_ID_CONFIG_TEST,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConstantInCombine.AUTO_OFFSET_RESET_CONFIG
      )
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        Set(topic)
      ).map(_._2)
    }

    val duration = args(0).toInt
    val ssc = new StreamingContext(sc, Durations.minutes(duration))
    ssc.checkpoint(ConstantInCombine.SPARK_STREAMING_CHECKPOINT_PATH + "/b2bTest" + ConstantInCombine.COMBINE_CHECKPOINT_PATH_NAME)

    val fonsviewDS = createKafkaSCTest(ssc, ConstantInCombine.FONSVIEW_KAFKA_B2B_TOPIC_NAME)

    val hanYanDS = createKafkaSCTest(ssc, ConstantInCombine.HANG_YAN_KAFKA_TOPIC_NAME)

    val zteDS = createKafkaSCTest(ssc, ConstantInCombine.ZTE_KAFKA_B2B_TOPIC_NAME)
    val fonsview = fonsviewDS.mapPartitions(rdd => {
      rdd.filter(line => {
        val json = JSON.parseObject(line)
        val message = json.getString("message")
        val messageSplits = message.split("\\|")
        val server_ip = messageSplits(1)
        val filesize = messageSplits(14)

        messageSplits.length == 46 &&
          !"".equals(messageSplits(0)) &&
          server_ip.contains(".") &&
          filesize.matches("[0-9]+")
      }).flatMap(line => {
        val json = JSON.parseObject(line)

        val messageSplits = json.getString("message").split("\\|")
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
        val sendtime = reqendtime - reqstarttime
        var firsttime = 0.0
        if (!"null".equalsIgnoreCase(messageSplits(17))) {
          val responsetime = tranTimeb2bfh(messageSplits(17)).toDouble
          firsttime = (responsetime - reqstarttime) / 1000 // 单位换成秒
        }
        val httpstatus = messageSplits(11)
        val upstream_httpstatus = messageSplits(41)
        val reqhitstatus = messageSplits(12)


        val vendor = ConstantInCombine.FONSVIEW_VENDOR_SIMPLE_NAME
        //        val vendor = "烽火"
        splitLineData(server_ip, reqdomain, filesize, upstream_filesize,
          reqstarttime, reqendtime, sendtime, firsttime, vendor,
          httpstatus, upstream_httpstatus, reqhitstatus)
      })
    })
    val hanYan = hanYanDS.mapPartitions(rdd => {
      rdd.filter(line => {
        val json = JSON.parseObject(line)
        val message = json.getString("message")
        val messageSplits = message.split("\\|")
        val server_ip = messageSplits(1)
        val filesize = messageSplits(14)
        val reqstarttime = messageSplits(15)
        //20190927T015352.158Z
        val reqendtime = messageSplits(16) //20190927T015352.371Z
        messageSplits.length == 64 &&
          !"".equals(messageSplits(0)) &&
          server_ip.contains(".") &&
          Pattern.matches(".*[a-zA-Z]+.*", reqstarttime) && //判断是否带字母过滤脏数据
          Pattern.matches(".*[a-zA-Z]+.*", reqendtime) && //判断是否带字母过滤脏数据
          filesize.matches("[0-9]+") //过滤脏数据TCP_MEM_HIT
      }).flatMap(line => {
        val json = JSON.parseObject(line)
        val messageSplits = json.getString("message").split("\\|")
        val server_ip = messageSplits(1)
        val reqdomain = messageSplits(6)
        val filesize = messageSplits(14).toDouble
        var upstream_filesize = 0D
        if (messageSplits(31).matches("[0-9]+")) {
          upstream_filesize = messageSplits(31).toDouble
        }
        val reqstarttime = tranTimeb2bhy(messageSplits(15))
        val reqendtime = tranTimeb2bhy(messageSplits(16))
        val sendtime = reqendtime - reqstarttime
        var firsttime = 0.0
        if (Pattern.matches(".*[a-zA-Z]+.*", messageSplits(17))) {
          val responsetime = tranTimeb2bhy(messageSplits(17)).toDouble
          firsttime = (responsetime - reqstarttime) / 1000 // 单位换成秒
        }
        val httpstatus = messageSplits(11)
        val upstream_httpstatus = messageSplits(41)
        val reqhitstatus = messageSplits(12)
        val vendor = ConstantInCombine.HANG_YAN_SIMPLE_NAME
        //        val vendor = "杭研"
        splitLineData(server_ip, reqdomain, filesize, upstream_filesize,
          reqstarttime, reqendtime, sendtime, firsttime, vendor,
          httpstatus, upstream_httpstatus, reqhitstatus)
      })
    })

    val zte = zteDS.mapPartitions(rdd => {
      rdd.filter(line => {
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
        //        && messageSplits(0).trim.contains(".") && Pattern.matches(".*\\.(com|cn)", messageSplits(1).trim)
        flag
      })
        .flatMap(line => {
          val json = JSON.parseObject(line)

          val messageSplits = json.getString("message").split("\"")
          val fisrttimes = messageSplits(2).trim.split(" ")
          val server_ip = messageSplits(0).trim
          val reqdomain = messageSplits(1).trim
          val firsttime = fisrttimes(0).toDouble
          val timestamp = tranTimeToLong(tranTimeZte(fisrttimes(1).drop(1)))
          val httpstatus_filesize = messageSplits(4).trim.split(" ")
          val httpstatus = httpstatus_filesize(0).trim
          val filesize = httpstatus_filesize(1).trim.toDouble
          val sendtime = messageSplits(9).trim.toDouble * 1000 // 单位秒
          val reqstarttime = timestamp - sendtime.toLong
          val reqendtime = timestamp // tranTimeToLong 返回的是毫秒 所以要乘1000
          val upstream_cache_status = messageSplits(11).trim
          val upstream_httpstatus = messageSplits(12).trim
          var upstream_filesize = 0.0 // 源数据中无，故全部置零
          if (upstream_cache_status.contains("MISS") && !httpstatus_filesize(2).trim.contains("-")) {
            val len = httpstatus_filesize.length
            upstream_filesize = httpstatus_filesize(len - 1).trim.toDouble

          }
          val vendor = ConstantInCombine.ZTE_VENDOR_SIMPLE_NAME
          //          val vendor = "中兴"
          splitLineData(server_ip, reqdomain, filesize, upstream_filesize,
            reqstarttime, reqendtime, sendtime.toLong, firsttime, vendor,
            httpstatus, upstream_httpstatus, upstream_cache_status)
        })
    })
    val unionSS = fonsview.union(hanYan).union(zte)
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
          |formatTimeTemp(timestamp) as format_timestamp,
          |server_ip,
          |reqdomain,
          |vendor,
          |sum(httpstatus_4xx) as httpstatus_4xx_count,
          |sum(httpststus_5xx) as httpstatus_5xx_count,
          |sum(upstream_4xx) as upstream_4xx_count,
          |sum(upstream_5xx) as upstream_5xx_count,
          |sum(hit_status) as sum_hitstatus,
          |sum(filesize) as sum_filesize,
          |sum(count) as total,
          |avg(firsttime) as sum_firsttime,
          |sum(hit_filesize) as sum_hitfilesize,
          |sum(upstream_filesize) as sum_upstream_filesize,
          |sum(sendtime) as sum_sendtime,
          |sum(download_filesize) as sum_download_filesize
          |from UnionDFTable group by timestamp,server_ip,reqdomain,vendor
          |""".stripMargin)
      finalDF.cache()
      val prop = new Properties()
      prop.put("driver", ConstantInCombine.MYSQL_DRIVER)
      prop.put("user", ConstantInCombine.MYSQL_USER)
      prop.put("password", ConstantInCombine.MYSQL_PASSWORD)
      //      finalDF.show(10)
      finalDF.write.mode(SaveMode.Overwrite).jdbc(ConstantInCombine.MYSQL_URL, ConstantInCombine.MYSQL_DATABLE_B2B_KPI_DWS_LUMIA, prop)
      finalDF.foreachPartition(iter => {
        val conn = DriverManager.getConnection(ConstantInCombine.MYSQL_URL, ConstantInCombine.MYSQL_USER, ConstantInCombine.MYSQL_PASSWORD)
        conn.setAutoCommit(false)
        val dbTable = ConstantInCombine.MYSQL_DATABLE_B2B_KPI_LUMIA
        val sql =
          s"""
             |INSERT INTO $dbTable (time_stamp,server_ip,reqdomain,
             |vendor,format_timestamp,httpstatus_4xx_count,httpstatus_5xx_count,
             |upstream_4xx_count,upstream_5xx_count,sum_hitstatus,sum_filesize,
             |total,sum_firsttime,sum_hitfilesize,sum_upstream_filesize,sum_sendtime,
             |sum_download_filesize)
             |VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
             |ON DUPLICATE KEY UPDATE sum_filesize=sum_filesize+?,
             |httpstatus_4xx_count=httpstatus_4xx_count+?,
             |httpstatus_5xx_count=httpstatus_5xx_count+?,
             |upstream_4xx_count=upstream_4xx_count+?,
             |upstream_5xx_count=upstream_5xx_count+?,
             |sum_hitstatus=sum_hitstatus+?,
             |total=total+?,
             |sum_firsttime=sum_firsttime+?,
             |sum_hitfilesize=sum_hitfilesize+?,
             |sum_upstream_filesize=sum_upstream_filesize+?,
             |sum_sendtime=sum_sendtime+?,
             |sum_download_filesize=sum_download_filesize+?
             |""".stripMargin
        //          val sql = s"insert into $dbTable (time_stamp,server_ip,total) values(?,?,?) ON DUPLICATE KEY UPDATE total=total+?"

        val stat = conn.prepareStatement(sql)

        iter.foreach(row => {

          val time_stamp = row.getAs[Long]("time_stamp")
          val server_ip = row.getAs[String]("server_ip")
          val reqdomain = row.getAs[String]("reqdomain")
          val vendor = row.getAs[String]("vendor")
          val format_timestamp = row.getAs[String]("format_timestamp")
          val httpstatus_4xx_count = row.getAs[Long]("httpstatus_4xx_count")
          val httpstatus_5xx_count = row.getAs[Long]("httpstatus_5xx_count")
          val upstream_4xx_count = row.getAs[Long]("upstream_4xx_count")
          val upstream_5xx_count = row.getAs[Long]("upstream_5xx_count")
          val sum_hitstatus = row.getAs[Long]("sum_hitstatus")
          val sum_filesize = row.getAs[Double]("sum_filesize")
          val total = row.getAs[Long]("total")
          val sum_firsttime = row.getAs[Double]("sum_firsttime")
          val sum_hitfilesize = row.getAs[Double]("sum_hitfilesize")
          val sum_upstream_filesize = row.getAs[Double]("sum_upstream_filesize")
          val sum_sendtime = row.getAs[Double]("sum_sendtime")
          val sum_download_filesize = row.getAs[Double]("sum_download_filesize")
          //        println(time_stamp)
          //        println(time_flag)
          //        println(server_ip)
          stat.setLong(1, time_stamp)
          stat.setString(2, server_ip)
          stat.setString(3, reqdomain)
          stat.setString(4, vendor)
          stat.setString(5, format_timestamp)
          stat.setLong(6, httpstatus_4xx_count)
          stat.setLong(7, httpstatus_5xx_count)
          stat.setLong(8, upstream_4xx_count)
          stat.setLong(9, upstream_5xx_count)
          stat.setLong(10, sum_hitstatus)
          stat.setDouble(11, sum_filesize)
          stat.setLong(12, total)
          stat.setDouble(13, sum_firsttime)
          stat.setDouble(14, sum_hitfilesize)
          stat.setDouble(15, sum_upstream_filesize)
          stat.setDouble(16, sum_sendtime)
          stat.setDouble(17, sum_download_filesize)
          stat.setDouble(18, sum_filesize)
          stat.setLong(19, httpstatus_4xx_count)
          stat.setLong(20, httpstatus_5xx_count)
          stat.setLong(21, upstream_4xx_count)
          stat.setLong(22, upstream_5xx_count)
          stat.setLong(23, sum_hitstatus)
          stat.setLong(24, total)
          stat.setDouble(25, sum_firsttime)
          stat.setDouble(26, sum_hitfilesize)
          stat.setDouble(27, sum_upstream_filesize)
          stat.setDouble(28, sum_sendtime)
          stat.setDouble(29, sum_download_filesize)

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
    conf.setAppName("comsumeAndCompute")
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "10000000")
    conf.set("spark.sql.shuffle.partitions", "100")
    conf.set("spark.shuffle.file.buffer", "64k")
    conf.set("spark.reducer.maxSizeInFlight", "64m")
    conf.set("spark.cleaner.ttl", "172800")
  }

  def calcuNewFilesize(consistTime: Long, filesize: Double, sendtime: Long): Double = {
    consistTime * filesize / sendtime
  }

  def handleStatus(filesize: Double, httpstatus: String, upstream_httpstatus: String, reqhitstatus: String): List[Double] = {
    var httpstatus_4xx = 0
    var httpstatus_5xx = 0
    var upstream_4xx = 0
    var upstream_5xx = 0
    var hit_status = 0
    var hit_filesize = 0.0
    if (httpstatus.startsWith("5") && upstream_httpstatus.startsWith("5")) {
      httpstatus_5xx = 1
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
    List[Double](httpstatus_4xx, httpstatus_5xx, upstream_4xx, upstream_5xx, hit_status, hit_filesize)
  }

  def splitLineData(server_ip: String, reqdomain: String, filesize: Double, upstream_filesize: Double,
                    reqstarttime: Long, reqendtime: Long, sendtime: Long, firsttime: Double,
                    vendor: String, httpstatus: String, upstream_httpstatus: String,
                    reqhitstatus: String): ListBuffer[B2BModel] = {

    val statusList = handleStatus(filesize, httpstatus, upstream_httpstatus, reqhitstatus)
    val httpstatus_4xx = statusList(0).toInt
    val httpstatus_5xx = statusList(1).toInt
    val upstream_4xx = statusList(2).toInt
    val upstream_5xx = statusList(3).toInt
    val hit_status = statusList(4).toInt
    val hit_filesize = statusList(5)

    val StartTimeFormat = formatFhStartLong(reqstarttime)
    val EndTimeFormat = formatFhEndLong(reqendtime)
    var i = 1
    var timestampTemp = StartTimeFormat
    val windowGap = 5 * 60 * 1000
    val count = (EndTimeFormat - StartTimeFormat) / windowGap
    val list = new ListBuffer[B2BModel]
    var filesizeNew: Double = filesize
    var download_filesizeNew: Double = filesize
    var upstream_filesizeNew: Double = upstream_filesize
    var hit_filesizeNew: Double = hit_filesize
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
          hit_filesizeNew = calcuNewFilesize(consistTimeTemp, hit_filesize, sendtime)
          list.append(B2BModel(
            timestampTemp,
            server_ip,
            reqdomain,
            filesizeNew,
            download_filesizeNew,
            upstream_filesizeNew,
            sendtime,
            firsttime,
            vendor,
            httpstatus_4xx,
            httpstatus_5xx,
            upstream_4xx,
            upstream_5xx,
            hit_status,
            1,
            hit_filesizeNew
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
      hit_filesizeNew = calcuNewFilesize(consistTime, hit_filesize, sendtime)
    } else {
      filesizeNew = filesize
      download_filesizeNew = 0
      upstream_filesizeNew = upstream_filesize
      hit_filesizeNew = hit_filesize
    }

    list.append(B2BModel(
      timestampTemp,
      server_ip,
      reqdomain,
      filesizeNew,
      download_filesizeNew,
      upstream_filesizeNew,
      sendtime,
      firsttime,
      vendor,
      httpstatus_4xx,
      httpstatus_5xx,
      upstream_4xx,
      upstream_5xx,
      hit_status,
      1,
      hit_filesizeNew
    ))
    list
  }
}
