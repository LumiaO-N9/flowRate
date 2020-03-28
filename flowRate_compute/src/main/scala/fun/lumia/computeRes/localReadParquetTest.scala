package fun.lumia.computeRes

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import fun.lumia.ComputeConstant
import fun.lumia.common.SparkTool
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.desc
import fun.lumia.bean.scalaClass
import fun.lumia.bean.scalaClass.{B2BModel, OttModel}

object localReadParquetTest extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {
    val sqlContext = sql
    import sqlContext.implicits._
    val path1 = "data/b2b/2019-11-21-10"
    val path2 = "data/ott/2019-11-14-15"
    val DF1 = sql.read.parquet(path1)
    DF1.show(10)
    DF1.registerTempTable("test")
    val testDF = sql.sql(
      """
        |select timestamp as time_stamp,
        |server_ip,
        |sum(count) as total
        |from test
        |group by timestamp,server_ip
        |""".stripMargin)
    testDF.foreachPartition(iter => {
      val conn = DriverManager.getConnection(ComputeConstant.MYSQL_URL, ComputeConstant.MYSQL_USER, ComputeConstant.MYSQL_PASSWORD)
      iter.foreach(row => {
        val time_stamp = row.getAs[Long]("time_stamp")
        val server_ip = row.getAs[String]("server_ip")
        val total = row.getAs[Long]("total")
        val test_table = "cdn.test_table"
        //        println(time_stamp)
        //        println(time_flag)
        //        println(server_ip)
        val sql = s"insert into $test_table (time_stamp,server_ip,total) values(?,?,?) ON DUPLICATE KEY UPDATE total=total+?"
        val stat = conn.prepareStatement(sql)
        stat.setLong(1, time_stamp)
        stat.setString(2, server_ip)
        stat.setLong(3, total)
        stat.setLong(4, total)
                println(stat)
        stat.executeUpdate()
        stat.close()
      })
      conn.close()
    })


    //    //    val DF2 = sql.read.format("parquet").load(path2)
    //    //    DF1.show(100)
    //    DF1.show(10)
    //    DF2.show(10)
    //    DF1.sample(false, 0.01).unionAll(emptyB2BDF).filter($"server_ip" === "null").show(1000)
    //    DF2.sample(false, 0.1).unionAll(emptyOttDF).filter($"server_ip" === "null").show()
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    conf.setMaster("local[8]")
  }
}
