package fun.lumia.computeRes

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import fun.lumia.ComputeConstant
import fun.lumia.common.SparkTool
import org.apache.spark.sql.SaveMode

object computeZteResFromHdfs extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    // 获取当前时间戳(毫秒)
    val currentTime = new Date().getTime()

    //当前计算时的时间区间
    val delayMinute: Int = 10 // minutes 延时时间
    val districtSize = ComputeConstant.COMPUTE_DISTRICT_SECONDS.toInt // seconds
    val timeDistrictStart = currentTime - (delayMinute * 60 + districtSize) * 1000
    val timeDistrictEnd = currentTime - delayMinute * 60 * 1000

    // 根据区间起始时间戳获取前一个小时时间时间戳(毫秒)
    val frontOneHourTime = timeDistrictStart - 3600 * 1000
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    // yyyy/MM/dd HH:mm:ss
    //    val dateMinuteFormat: SimpleDateFormat = new SimpleDateFormat("mm")
    val currentHourStr = dateFormat.format(timeDistrictStart)
    val frontOneHourStr = dateFormat.format(frontOneHourTime)
    val pathStr = ComputeConstant.SPARK_STREAMING_WRITE_PATH + "/" + ComputeConstant.ZTE_KAFKA_B2B_TOPIC_NAME + "/"
    val frontOneHourPath = pathStr + frontOneHourStr
    val currentHourPath = pathStr + currentHourStr


    //    println("***************timeDistrictStart**************")
    //    println(timeDistrictStart)
    //    println("***************timeDistrictEnd**************")
    //    println(timeDistrictEnd)
    // 读取前一个小时的目录
    val frontOneHourHyDF = sql.read.format("parquet").load(frontOneHourPath)
    // 读取当前小时的目录
    val currentHourHyDF = sql.read.format("parquet").load(currentHourPath)
    //    println("***************frontOneHourHyDF**************")
    //    frontOneHourHyDF.show(100)
    //    println("***************currentHourHyDF**************")
    //    currentHourHyDF.show(100)

    val DF = frontOneHourHyDF.unionAll(currentHourHyDF)

    //    DF.show(100)
    //    println("********************")
    // 28800000‬
    val min = timeDistrictStart
    val max = timeDistrictEnd
    //    DF.filter($"reqstarttime" > min || $"reqendtime>1571283596000" > max).show()
    DF.registerTempTable("DFTable")
    val filterSqlStr = "select * from DFTable where ( reqstarttime>=" + min + " and reqstarttime<=" + max + ") or (reqendtime>=" + min + " and reqendtime<=" + max + ") or (reqstarttime<=" + min + " and reqendtime>=" + max + ")"
    //    println(filterSqlStr)
    //    val filterDF
    val filterDF = sql.sql(filterSqlStr)
    filterDF.registerTempTable("filterDFTable")
    //    sql.udf.register()
    sql.sql(
      """
        |select current_timestamp() as time_stamp,server_ip,
        |reqdomain,
        |city,
        |"zte" as vendor,
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
        |from filterDFTable group by server_ip,reqdomain,city
        |""".stripMargin).registerTempTable("groupDFTable")


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
        |city,
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
        |""".stripMargin).registerTempTable("finalDFTable")

    sql.sql(
      """
        |select * from finalDFTable a left join domainTable b on a.reqdomain = b.DOMAIN
        |""".stripMargin).registerTempTable("joinDFTable")
    val lastDF = sql.sql(
      """
        |select
        |time_stamp,
        |server_ip,
        |reqdomain,
        |city,
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
        |from joinDFTable
        |""".stripMargin)
    // 将最终结果写入MySQL
    //    lastDF.show()
    val prop = new Properties()
    prop.put("driver", ComputeConstant.MYSQL_DRIVER)
    prop.put("user", ComputeConstant.MYSQL_USER)
    prop.put("password", ComputeConstant.MYSQL_PASSWORD)
    lastDF.write.mode(SaveMode.Append).jdbc(ComputeConstant.MYSQL_URL, ComputeConstant.MYSQL_DATABLE_COPY, prop)

  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    //    conf.setMaster("local[4]")
  }
}
