package fun.lumia.computeRes

import fun.lumia.common.SparkTool
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.spark.sql._

object localWriteES extends SparkTool {

  /**
   * 在run方法里面编写spark业务逻辑
   */
  case class Person(name: String, surname: String, age: Int)

  override def run(args: Array[String]): Unit = {
    //  create DataFrame
    val sqlContext = sql
    import sqlContext.implicits._
    val people = sc.textFile("data/people.txt")
      .map(_.split(","))
      .map(line => {
        val name = line(0)
        val surname = line(1)
        val age = line(2).trim.toInt
        Person(name, surname, age)
      }).toDF()
    //      .map(_.split(","))
    //      .map(p => Person(p(0), p(1), p(2).trim.toInt))
    //      .toDF()
    //

    people.saveToEs("spark/people")
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    conf.setMaster("local[8]")
    //指定es配置

    conf.set("cluster.name", "fh_topic")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "112.29.140.223:9200")
    conf.set("es.port","9200")
    conf.set("es.index.read.missing.as.empty", "true")
    conf.set("es.nodes.wan.only", "true")
  }
}
