package fun.lumia.computeRes

import java.sql.DriverManager
import java.util.Properties

import fun.lumia.ComputeConstant
import fun.lumia.common.SparkTool
import fun.lumia.util.loadDFfunctions.{getJudgeTime, loadUnionDFByVendor}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object computeAllB2BFromHdfsTest extends SparkTool {


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
    var defaultRes = 100000000
    if (resParameter1 >= 0) {
      defaultRes = resParameter1
    }
    //    DF.filter($"reqstarttime" > min || $"reqendtime>1571283596000" > max).show()
    val judgeTime = getJudgeTime()
    val b2bPathStr = ComputeConstant.SPARK_STREAMING_WRITE_PATH
    val unionDF = createUnionDF(judgeTime, b2bPathStr, sc, sql).coalesce(200)
    unionDF.registerTempTable("DFTable")
    val min = judgeTime
    val max = min + 60 * 1000
    val filterSqlStr = "select * from DFTable where res<=" + defaultRes + " and (( reqstarttime>=" + min + " and reqstarttime<=" + max + ") or (reqendtime>=" + min + " and reqendtime<=" + max + ") or (reqstarttime<=" + min + " and reqendtime>=" + max + "))"
    //    val filterSqlStr = "select * from DFTable where res<=" + defaultRes + " and " + judgeTime + ">=reqstarttime and reqendtime>=" + judgeTime
    //    println(filterSqlStr)
    //    val filterDF
    val filterDF = sql.sql(filterSqlStr)
    filterDF.registerTempTable("filterDFTable")
    //    sql.udf.register()
    sql.sql(
      """
        |select *,
        |(reqendtime-reqstarttime)/1000 as sendtime
        |from filterDFTable
        |""".stripMargin).coalesce(200).registerTempTable("newTempTable")
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
        |sum(filesize/1024/1024)/sum(sendtime) as download_rate_sum_new,
        |sum(count) as total,
        |max(firsttime) as firsttime_max,
        |avg(firsttime) as firsttime_avg,
        |sum(hit_filesize) as hit_filesize,
        |sum(filesize) as filesize
        |from newTempTable group by server_ip,reqdomain,vendor
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
        |download_rate_sum_new,
        |filesize,
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
        |download_rate_sum_new,
        |filesize,
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

    lastDF.write.mode(SaveMode.Append).jdbc(ComputeConstant.MYSQL_URL, ComputeConstant.MYSQL_DATABLE_B2B_TOPIC_TEST, prop)

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
