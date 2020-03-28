package fun.lumia.computeOttRes

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import fun.lumia.ComputeConstant
import fun.lumia.bean.scalaClass.OttModel
import fun.lumia.common.SparkTool
import fun.lumia.util.loadDFfunctions.{getJudgeTime, loadUnionDFByVendor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object computeOttAllFromHdfs extends SparkTool {
  //  def ifPathExists(path: String, sparkContext: SparkContext): Boolean = {
  //    val hadoopConf: Configuration = sparkContext.hadoopConfiguration
  //    val fs = FileSystem.get(hadoopConf)
  //    fs.exists(new Path(path))
  //  }
  def createUnionDF(judgeTime: Long, frontPath: String, sparkContext: SparkContext, sqlContext: SQLContext): DataFrame = {
    val fonsviewOttUnionDF = loadUnionDFByVendor(judgeTime, frontPath, ComputeConstant.FONSVIEW_VENDOR_NAME, sparkContext, sqlContext)
    val hangYanOttUnionDF = loadUnionDFByVendor(judgeTime, frontPath, ComputeConstant.HUAWEI_VENDOR_NAME, sparkContext, sqlContext)
    val zteOttUnionDF = loadUnionDFByVendor(judgeTime, frontPath, ComputeConstant.ZTE_VENDOR_NAME, sparkContext, sqlContext)
    fonsviewOttUnionDF.unionAll(hangYanOttUnionDF).unionAll(zteOttUnionDF)
  }

  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    // 获取当前时间戳(毫秒)
    val currentTime = new Date().getTime()

    //当前计算时的时间区间
    //    val districtSizeMillisecond = ComputeConstant.COMPUTE_DISTRICT_MILLISECONDS.toInt // seconds
    //    val timeDistrictStart = currentTime - delayMinute * 60 * 1000 - districtSizeMillisecond
    //    val timeDistrictEnd = currentTime - delayMinute * 60 * 1000


    //    val delayMinute: Int = 30 // minutes
    //    val secondFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    //    val currentTimeSecond = secondFormat.parse(secondFormat.format(currentTime)).getTime()
    //    val delayTimeSecond = currentTimeSecond - delayMinute * 60 * 1000

    // 根据区间起始时间戳获取前一个小时时间时间戳(毫秒)
    //    val frontOneHourTime = delayTimeSecond - 3600 * 1000
    //    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")

    // yyyy/MM/dd HH:mm:ss
    //    val dateMinuteFormat: SimpleDateFormat = new SimpleDateFormat("mm")
    //    val currentHourStr = "/" + dateFormat.format(delayTimeSecond)
    //    val frontOneHourStr = "/" + dateFormat.format(frontOneHourTime)
    //    val ottPathStr = ComputeConstant.SPARK_STREAMING_OTT_WRITE_PATH

    // 构建Ott读取目录
    // 烽火目录
    //    val fonsviewFrontOneHourPath = ottPathStr + "/" + ComputeConstant.FONSVIEW_VENDOR_NAME + frontOneHourStr
    //    val fonsviewCurrentHourPath = ottPathStr + "/" + ComputeConstant.FONSVIEW_VENDOR_NAME + currentHourStr
    //    // 华为目录
    //    val huaweiFrontOneHourPath = ottPathStr + "/" + ComputeConstant.HUAWEI_VENDOR_NAME + frontOneHourStr
    //    val huaweiCurrentHourPath = ottPathStr + "/" + ComputeConstant.HUAWEI_VENDOR_NAME + currentHourStr
    //    // 中兴目录
    //    val zteFrontOneHourPath = ottPathStr + "/" + ComputeConstant.ZTE_VENDOR_NAME + frontOneHourStr
    //    val zteCurrentHourPath = ottPathStr + "/" + ComputeConstant.ZTE_VENDOR_NAME + currentHourStr

    //    // 读取烽火前一个小时和当前小时的目录
    //    val frontOneHourFonsviewDF = sql.read.parquet(fonsviewFrontOneHourPath)
    //    val currentHourFonsviewDF = sql.read.parquet(fonsviewCurrentHourPath)
    //    // 读取华为前一个小时和当前小时的目录
    //    val frontOneHourHuaweiDF = sql.read.parquet(huaweiFrontOneHourPath)
    //    val currentHourHuaweiDF = sql.read.parquet(huaweiCurrentHourPath)
    //    // 读取中兴前一个小时和当前小时的目录
    //    val frontOneHourZteDF = sql.read.parquet(zteFrontOneHourPath)
    //    val currentHourZteDF = sql.read.parquet(zteCurrentHourPath)
    //
    //    var frontOneHourFonsviewDF = createEmptyOttModelDF(sql)
    //    var currentHourFonsviewDF = createEmptyOttModelDF(sql)
    //    var frontOneHourHuaweiDF = createEmptyOttModelDF(sql)
    //    var currentHourHuaweiDF = createEmptyOttModelDF(sql)
    //    var frontOneHourZteDF = createEmptyOttModelDF(sql)
    //    var currentHourZteDF = createEmptyOttModelDF(sql)
    //
    //    // 读取烽火前一个小时和当前小时的目录
    //    if (ifPathExists(fonsviewFrontOneHourPath, sc)) {
    //      frontOneHourFonsviewDF = sql.read.parquet(fonsviewFrontOneHourPath)
    //    }
    //    if (ifPathExists(fonsviewCurrentHourPath, sc)) {
    //      currentHourFonsviewDF = sql.read.parquet(fonsviewCurrentHourPath)
    //    }
    //
    //    // 读取华为前一个小时和当前小时的目录
    //    if (ifPathExists(huaweiFrontOneHourPath, sc)) {
    //      frontOneHourHuaweiDF = sql.read.parquet(huaweiFrontOneHourPath)
    //    }
    //    if (ifPathExists(huaweiCurrentHourPath, sc)) {
    //      currentHourHuaweiDF = sql.read.parquet(huaweiCurrentHourPath)
    //    }
    //
    //    // 读取中兴前一个小时和当前小时的目录
    //    if (ifPathExists(zteFrontOneHourPath, sc)) {
    //      frontOneHourZteDF = sql.read.parquet(zteFrontOneHourPath)
    //    }
    //    if (ifPathExists(zteCurrentHourPath, sc)) {
    //      currentHourZteDF = sql.read.parquet(zteCurrentHourPath)
    //    }
    //
    //    val unionDF = frontOneHourFonsviewDF.unionAll(currentHourFonsviewDF)
    //      .unionAll(frontOneHourHuaweiDF)
    //      .unionAll(currentHourHuaweiDF)
    //      .unionAll(frontOneHourZteDF)
    //      .unionAll(currentHourZteDF).coalesce(100)
    val judgeTime = getJudgeTime()
    val ottPathStr = ComputeConstant.SPARK_STREAMING_OTT_WRITE_PATH
    val unionDF = createUnionDF(judgeTime, ottPathStr, sc, sql).coalesce(100)

    unionDF.registerTempTable("unionDFTable")

    //    val min = timeDistrictStart
    //    val max = timeDistrictEnd

    val filterFirsttimeStr = "select * from unionDFTable where firsttime<=5000"
    sql.sql(filterFirsttimeStr).registerTempTable("filterUnionDFTable")
    //    val filterSqlStr = "select * from filterUnionDFTable where ( starttime>=" + min + " and starttime<=" + max + ") or (endtime>=" + min + " and endtime<=" + max + ") or (starttime<=" + min + " and endtime>=" + max + ")"
    val filterSqlStr = "select * from filterUnionDFTable where " + judgeTime + ">=starttime and endtime>=" + judgeTime

    val filterDF = sql.sql(filterSqlStr)
    filterDF.registerTempTable("filterDFTable")

    sql.sql(
      """
        |select current_timestamp() as time_stamp,
        |server_ip,
        |vendor,
        |viewtype,
        |max(firsttime/1000) as packet_delay,
        |avg(firsttime/1000) as avg_packet_delay,
        |sum(res) as download_rate_sum,
        |sum(httpstatus_2xx) as httpstatus_2xx_count,
        |sum(httpstatus_3xx) as httpstatus_3xx_count,
        |sum(httpstatus_4xx) as httpstatus_4xx_count,
        |sum(httpstatus_5xx) as httpstatus_5xx_count,
        |sum(upstatus_5xx) as upstatus_5xx_count,
        |sum(total) as total
        |from filterDFTable group by server_ip,viewtype,vendor
        |""".stripMargin).coalesce(100).registerTempTable("groupDFTable")


    // 读取MySQL中cmdb 表做关联
    val cmdbDF = sql.read.format("jdbc") //指定读取数据格式
      .options(Map( //指定参数
        "url" -> ComputeConstant.MYSQL_URL,
        "driver" -> ComputeConstant.MYSQL_DRIVER,
        "dbtable" -> ComputeConstant.MYSQL_DBTABLE_CMDB,
        "user" -> ComputeConstant.MYSQL_USER,
        "password" -> ComputeConstant.MYSQL_PASSWORD
      )).load() //加载数据
    val selectCMDBDF = cmdbDF.select("REGION", "BUSI_IPV4", "BUSI_GRPNAME")
    selectCMDBDF.registerTempTable("selectCMDBTable")
    sql.sql(
      """
        |select * from groupDFTable a left join selectCMDBTable b on a.server_ip = b.BUSI_IPV4
        |""".stripMargin).coalesce(100).registerTempTable("joinDFTable")
    val lastDF = sql.sql(
      """
        |select
        |date_format(time_stamp,"yyyy-MM-dd HH:mm") as time_stamp,
        |server_ip as cache_server_ip,
        |REGION as city,
        |vendor as soft_factory,
        |BUSI_GRPNAME as busi_grpname,
        |viewtype,
        |packet_delay,
        |avg_packet_delay,
        |download_rate_sum,
        |httpstatus_2xx_count,
        |httpstatus_3xx_count,
        |httpstatus_4xx_count,
        |httpstatus_5xx_count,
        |upstatus_5xx_count,
        |total
        |from joinDFTable
        |""".stripMargin).coalesce(100)
    // 将最终结果写入MySQL
    val prop = new Properties()
    prop.put("driver", ComputeConstant.MYSQL_DRIVER)
    prop.put("user", ComputeConstant.MYSQL_USER)
    prop.put("password", ComputeConstant.MYSQL_PASSWORD)
    lastDF.write.mode(SaveMode.Append).jdbc(ComputeConstant.MYSQL_URL, ComputeConstant.MYSQL_DBTABLE_OTT, prop)

  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    //    conf.setMaster("local[4]")
  }


}
