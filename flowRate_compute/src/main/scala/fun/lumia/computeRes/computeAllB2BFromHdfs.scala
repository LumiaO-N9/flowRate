package fun.lumia.computeRes

import java.sql.DriverManager
import java.util.Properties

import fun.lumia.ComputeConstant
import fun.lumia.util.loadDFfunctions.{getJudgeTime, loadUnionDFByVendor}
import fun.lumia.common.SparkTool
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object computeAllB2BFromHdfs extends SparkTool {


  def createUnionDF(judgeTime: Long, frontPath: String, sparkContext: SparkContext, sqlContext: SQLContext): DataFrame = {
    val fonsviewUnionDF = loadUnionDFByVendor(judgeTime, frontPath, ComputeConstant.FONSVIEW_VENDOR_NAME, sparkContext, sqlContext)
    val hangYanUnionDF = loadUnionDFByVendor(judgeTime, frontPath, ComputeConstant.HANGYAN_VENDOR_NAME, sparkContext, sqlContext)
    val zteUnionDF = loadUnionDFByVendor(judgeTime, frontPath, ComputeConstant.ZTE_VENDOR_NAME, sparkContext, sqlContext)
    fonsviewUnionDF.unionAll(hangYanUnionDF).unionAll(zteUnionDF)
  }

  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    val resParameter1 = args(0).toInt
//        val millisecondParameter1 = args(0).toInt
//        val resParameter2 = args(1).toDouble
//        println("*" * 200)
//        println(millisecondParameter1)
//        println(resParameter2)
//        println("*" * 200)
//     获取当前时间戳(毫秒)
//        val currentTime = new Date().getTime()
//        val currentMinuteFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
//        val currentMinute = currentMinuteFormat.format(currentTime)
//    当前计算时的时间区间
//
//        val delayMinute: Int = 60 // minutes
//        val secondFormat = new SimpleDateFormat("yyyyMMddHHmmss")
//        val currentTimeSecond = secondFormat.parse(secondFormat.format(currentTime)).getTime()
//        val delayTimeSecond = currentTimeSecond - delayMinute * 60 * 1000
//
//        var districtSizeMillisecond = ComputeConstant.COMPUTE_DISTRICT_MILLISECONDS.toInt // seconds
//        if (millisecondParameter1 != 0) {
//          districtSizeMillisecond = millisecondParameter1
//        }
//        val timeDistrictEnd = currentTimeSecond - delayMinute * 60 * 1000
//        val timeDistrictStart = timeDistrictEnd - districtSizeMillisecond
//
//     根据区间起始时间戳获取前一个小时时间时间戳(毫秒)
//        val frontOneHourTime = timeDistrictEnd - 3600 * 1000
//        val frontTwoHourTime = frontOneHourTime - 3600 * 1000
//        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
//        val b2bPathStr = ComputeConstant.SPARK_STREAMING_WRITE_PATH
//
//        val currentHourStr = "/" + dateFormat.format(timeDistrictEnd)
//        val frontOneHourStr = "/" + dateFormat.format(frontOneHourTime)
//        val frontTwoHourStr = "/" + dateFormat.format(frontTwoHourTime)
//        // 构建B2B读取目录
//        // 烽火目录
//        val fonsviewFrontOneHourPath = b2bPathStr + "/" + ComputeConstant.FONSVIEW_VENDOR_NAME + frontOneHourStr
//        val fonsviewFrontTwoHourPath = b2bPathStr + "/" + ComputeConstant.FONSVIEW_VENDOR_NAME + frontTwoHourStr
//        val fonsviewCurrentHourPath = b2bPathStr + "/" + ComputeConstant.FONSVIEW_VENDOR_NAME + currentHourStr
//        // 杭研目录
//        val hangyanFrontOneHourPath = b2bPathStr + "/" + ComputeConstant.HANGYAN_VENDOR_NAME + frontOneHourStr
//        val hangyanFrontTwoHourPath = b2bPathStr + "/" + ComputeConstant.HANGYAN_VENDOR_NAME + frontTwoHourStr
//        val hangyanCurrentHourPath = b2bPathStr + "/" + ComputeConstant.HANGYAN_VENDOR_NAME + currentHourStr
//        // 中兴目录
//        val zteFrontOneHourPath = b2bPathStr + "/" + ComputeConstant.ZTE_VENDOR_NAME + frontOneHourStr
//        val zteFrontTwoHourPath = b2bPathStr + "/" + ComputeConstant.ZTE_VENDOR_NAME + frontTwoHourStr
//        val zteCurrentHourPath = b2bPathStr + "/" + ComputeConstant.ZTE_VENDOR_NAME + currentHourStr
//
//
//        var frontOneHourFonsviewDF = createEmptyB2BModelDF(sql)
//        var frontTwoHourFonsviewDF = createEmptyB2BModelDF(sql)
//        var currentHourFonsviewDF = createEmptyB2BModelDF(sql)
//        var frontOneHourHangYanDF = createEmptyB2BModelDF(sql)
//        var frontTwoHourHangYanDF = createEmptyB2BModelDF(sql)
//        var currentHourHangYanDF = createEmptyB2BModelDF(sql)
//        var frontOneHourZteDF = createEmptyB2BModelDF(sql)
//        var frontTwoHourZteDF = createEmptyB2BModelDF(sql)
//        var currentHourZteDF = createEmptyB2BModelDF(sql)
//
//        // 读取烽火前两个小时和当前小时的目录
//        if (ifPathExists(fonsviewFrontOneHourPath, sc)) {
//          frontOneHourFonsviewDF = sql.read.parquet(fonsviewFrontOneHourPath)
//        }
//        if (ifPathExists(fonsviewFrontTwoHourPath, sc)) {
//          frontTwoHourFonsviewDF = sql.read.parquet(fonsviewFrontTwoHourPath)
//        }
//        if (ifPathExists(fonsviewCurrentHourPath, sc)) {
//          currentHourFonsviewDF = sql.read.parquet(fonsviewCurrentHourPath)
//        }
//
//        // 读取杭研前一个小时和当前小时的目录
//        if (ifPathExists(hangyanFrontOneHourPath, sc)) {
//          frontOneHourHangYanDF = sql.read.parquet(hangyanFrontOneHourPath)
//        }
//        if (ifPathExists(hangyanFrontTwoHourPath, sc)) {
//          frontTwoHourHangYanDF = sql.read.parquet(hangyanFrontTwoHourPath)
//        }
//        if (ifPathExists(hangyanCurrentHourPath, sc)) {
//          currentHourHangYanDF = sql.read.parquet(hangyanCurrentHourPath)
//        }
//
//        // 读取中兴前一个小时和当前小时的目录
//        if (ifPathExists(zteFrontOneHourPath, sc)) {
//          frontOneHourZteDF = sql.read.parquet(zteFrontOneHourPath)
//        }
//        if (ifPathExists(zteFrontTwoHourPath, sc)) {
//          frontTwoHourZteDF = sql.read.parquet(zteFrontTwoHourPath)
//        }
//        if (ifPathExists(zteCurrentHourPath, sc)) {
//          currentHourZteDF = sql.read.parquet(zteCurrentHourPath)
//        }
//
//        val unionDF = frontOneHourFonsviewDF
//          .unionAll(frontTwoHourFonsviewDF)
//          .unionAll(currentHourFonsviewDF)
//          .unionAll(frontOneHourHangYanDF)
//          .unionAll(frontTwoHourHangYanDF)
//          .unionAll(currentHourHangYanDF)
//          .unionAll(frontOneHourZteDF)
//          .unionAll(frontTwoHourZteDF)
//          .unionAll(currentHourZteDF).coalesce(200)

    //    DF.show(100)
    //    println("********************")
    // 28800000‬
    //    val min = timeDistrictStart
    //    val max = timeDistrictEnd
    var defaultRes = 100000000
    if (resParameter1 >= 0) {
      defaultRes = resParameter1
    }
    //    DF.filter($"reqstarttime" > min || $"reqendtime>1571283596000" > max).show()
    val judgeTime = getJudgeTime()
    val b2bPathStr = ComputeConstant.SPARK_STREAMING_WRITE_PATH
    val unionDF = createUnionDF(judgeTime, b2bPathStr, sc, sql).coalesce(200)
    unionDF.registerTempTable("DFTable")
    //    val filterSqlStr = "select * from DFTable where res<=" + defaultRes + " and (( reqstarttime>=" + min + " and reqstarttime<=" + max + ") or (reqendtime>=" + min + " and reqendtime<=" + max + ") or (reqstarttime<=" + min + " and reqendtime>=" + max + "))"
    val filterSqlStr = "select * from DFTable where res<=" + defaultRes + " and " + judgeTime + ">=reqstarttime and reqendtime>=" + judgeTime
    //    println(filterSqlStr)
    //    val filterDF
    val filterDF = sql.sql(filterSqlStr)
    filterDF.registerTempTable("filterDFTable")
    //    sql.udf.register()
    sql.sql(
      """
        |select current_timestamp() as time_stamp,
        |server_ip,
        |reqdomain,
        |vendor,
        |sum(httpstatus_4xx) as httpstatus_4xx_count,
        |sum(httpststus_5xx) as httpstatus_5xx_count,
        |sum(upstream_4xx) as upstream_4xx_count,
        |sum(upstream_5xx) as upstream_5xx_count,
        |sum(hit_status) as hit_status_count,
        |sum(upres) as upstream_rate,
        |sum(res) as download_rate_sum,
        |avg(res) as download_rate_avg,
        |sum(count) as total,
        |max(firsttime) as firsttime_max,
        |avg(firsttime) as firsttime_avg,
        |sum(hit_filesize) as hit_filesize,
        |sum(filesize) as filesize
        |from filterDFTable group by server_ip,reqdomain,vendor
        |""".stripMargin).coalesce(200).registerTempTable("groupDFTable")


    // 读取MySQL中domain表做关联
    val domainDF = sql.read.format("jdbc") //指定读取数据格式
      .options(Map( //指定参数
        "url" -> ComputeConstant.MYSQL_URL,
        "driver" -> ComputeConstant.MYSQL_DRIVER,
        "dbtable" -> ComputeConstant.MYSQL_DBTABLE,
        "user" -> ComputeConstant.MYSQL_USER,
        "password" -> ComputeConstant.MYSQL_PASSWORD
      ))
      .load() //加载数据
    domainDF.registerTempTable("domainTable")
    sql.sql(
      """
        |select
        |time_stamp,
        |server_ip,
        |vendor,
        |reqdomain,
        |httpstatus_4xx_count,
        |httpstatus_5xx_count,
        |upstream_4xx_count,
        |upstream_5xx_count,
        |hit_status_count/total as hit_rate,
        |upstream_rate,
        |download_rate_sum,
        |download_rate_avg,
        |total,
        |firsttime_max,
        |firsttime_avg,
        |hit_filesize/filesize as hitbyte
        |from groupDFTable
        |""".stripMargin).coalesce(200).registerTempTable("finalDFTable")

    sql.sql(
      """
        |select * from finalDFTable a left join domainTable b on a.reqdomain = b.DOMAIN
        |""".stripMargin).coalesce(200).registerTempTable("joinDFTable")


    // 读取MySQL中cmdb 表做关联
    val cmdbDF = sql.read.format("jdbc") //指定读取数据格式
      .options(Map( //指定参数
        "url" -> ComputeConstant.MYSQL_URL,
        "driver" -> ComputeConstant.MYSQL_DRIVER,
        "dbtable" -> ComputeConstant.MYSQL_DBTABLE_CMDB,
        "user" -> ComputeConstant.MYSQL_USER,
        "password" -> ComputeConstant.MYSQL_PASSWORD
      )).load() //加载数据
    val selectCMDBDF = cmdbDF.select("REGION", "BUSI_IPV4")
    selectCMDBDF.registerTempTable("selectCMDBTable")

    sql.sql(
      """
        |select * from joinDFTable a left join selectCMDBTable b on a.server_ip = b.BUSI_IPV4
        |""".stripMargin).coalesce(200).registerTempTable("joinDFTable2")

    // 将最终结果写入MySQL
    val lastDF = sql.sql(
      """
        |select
        |date_format(time_stamp,"yyyy-MM-dd HH:mm:00") as time_stamp,
        |server_ip,
        |reqdomain,
        |REGION as city,
        |vendor,
        |BUSI_NAME as busi_name,
        |httpstatus_4xx_count,
        |httpstatus_5xx_count,
        |upstream_4xx_count,
        |upstream_5xx_count,
        |hit_rate,
        |upstream_rate,
        |download_rate_sum,
        |download_rate_avg,
        |total,
        |firsttime_max,
        |firsttime_avg,
        |hitbyte
        |from joinDFTable2
        |""".stripMargin).coalesce(200)

    val prop = new Properties()
    prop.put("driver", ComputeConstant.MYSQL_DRIVER)
    prop.put("user", ComputeConstant.MYSQL_USER)
    prop.put("password", ComputeConstant.MYSQL_PASSWORD)

    lastDF.write.mode(SaveMode.Append).jdbc(ComputeConstant.MYSQL_URL, ComputeConstant.MYSQL_DATABLE_B2B_TOPIC, prop)
    // 使用truncate清空dws表，防止字段类型改变
    Class.forName(ComputeConstant.MYSQL_DRIVER)
    val conn = DriverManager.getConnection(ComputeConstant.MYSQL_URL, prop)
    val sm = conn.prepareCall("truncate table " + ComputeConstant.MYSQL_DATABLE_DWS)
    sm.execute()
    sm.close()
    conn.close()
    lastDF.write.mode(SaveMode.Append).jdbc(ComputeConstant.MYSQL_URL, ComputeConstant.MYSQL_DATABLE_DWS, prop)
    //    //    }
    //    import org.elasticsearch.spark.sql._
    //    lastDF.saveToEs("b2btopicindex/allData")
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    //    conf.setMaster("local[4]")fh_topic
    //指定es配置
    //    conf.set("cluster.name", "fh_topic")
    //    conf.set("es.index.auto.create", "true")
    //    conf.set("es.nodes", "node223")
    //    conf.set("es.port", "9200")
    //    conf.set("es.index.read.missing.as.empty", "true")
    //    conf.set("es.nodes.wan.only", "true")
  }


}
