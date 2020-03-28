package fun.lumia.kafkaToHdfs

import java.text.SimpleDateFormat
import java.util.Calendar

import fun.lumia.common.SparkTool


object localReadParquetTest extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {

    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
    val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
    //    val path = "data/parquet/hy/" + dateStr
    //    val path = "data/parquet/fonsview/" + dateStr
    val path = "data/parquet/zte/" + dateStr
    val DF = sql.read.format("parquet").load(path)
        val DF2 = sql.read.format("parquet").load("data/parquet/zte/2019-11-06-20")
    DF.show(100)
    println("********************")
    var min = 1571283597000L
    var max = 1571283598000L
    //    DF.filter($"reqstarttime" > min || $"reqendtime>1571283596000" > max).show()
    DF.registerTempTable("DF")
    val filterSqlStr = "select * from DF where ( reqstarttime>" + min + " and reqstarttime<" + max + ") or (reqendtime>" + min + " and reqendtime<" + max + ")"
    println(filterSqlStr)
    //    val filterDF
    val filterDF = sql.sql(filterSqlStr)
    filterDF.show()
    filterDF.registerTempTable("filterDF")
//    sql.sql(
//      """
//        |select * from
//        |""".stripMargin)
    // 读取MySQL中domain表做关联
    val domainDF = sql.read.format("jdbc") //指定读取数据格式
      .options(Map( //指定参数
        "url" -> "com.mysql.jdbc.Driver",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "cdn.tb_res_domian",
        "user" -> "cdn",
        "password" -> "cdn@2019"
      ))
      .load() //加载数据

    domainDF.show()
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    conf.setMaster("local[4]")
  }
}
