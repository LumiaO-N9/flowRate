package fun.lumia.kafkaToHdfs

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import fun.lumia.utils.DateUtils._
import com.alibaba.fastjson.JSON
import fun.lumia.Constant
import fun.lumia.bean.scalaClass.B2BModel
import fun.lumia.common.SparkTool
import org.apache.spark.sql.SaveMode

object localConsumeTest extends SparkTool {
  /**
   * 在run方法里面编写spark业务逻辑
   */
  override def run(args: Array[String]): Unit = {

    val hySourceRDD = sc.textFile("data/testData/zteb2b.txt")
    val sqlContext = sql
    import sqlContext.implicits._
    // filter 不规则数据
    val hyRDD = hySourceRDD.filter(line => {
      //      val json = JSON.parseObject(line)
      //      val message = json.getString("message")
      //      val messageSplits = message.split("\\|")
      //      val server_ip = messageSplits(1)
      //      val filesize = messageSplits(14)
      //      val reqstarttime = messageSplits(15)
      //      //20190927T015352.158Z
      //      val reqendtime = messageSplits(16) //20190927T015352.371Z
      //      messageSplits.length == 64 &&
      //        !"".equals(messageSplits(0)) &&
      //        server_ip.contains(".") &&
      //        Pattern.matches(".*[a-zA-Z]+.*", reqstarttime) && //判断是否带字母过滤脏数据
      //        Pattern.matches(".*[a-zA-Z]+.*", reqendtime) && //判断是否带字母过滤脏数据
      //        filesize.matches("[0-9]+") //过滤脏数据TCP_MEM_HIT
      //    }).map(line => { // 取出需要的字段并转换成DataFrame，最后以parquet格式存储
      //      val json = JSON.parseObject(line)
      //      /**
      //       * {"@timestamp":"2019-10-17T03:39:56.780Z",
      //       * "@metadata":{"beat":"filebeat","type":"doc","version":"6.4.2","topic":"hy"},
      //       * "tags":["合肥","39.134.120.6"],
      //       * "prospector":{"type":"log"},
      //       * "input":{"type":"log"},
      //       * "beat":{"name":"ah2-cmcdn5.ahyd.cmcdn.net","hostname":"ah2-cmcdn5.ahyd.cmcdn.net","version":"6.4.2"},
      //       * "host":{"name":"ah2-cmcdn5.ahyd.cmcdn.net"},
      //       * "offset":5575078,
      //       * "message":"20191017T033955Z|39.134.120.34|10.193.145.45|39.134.120.6|GET|HTTP/1.1|valipl.cp12.wasu.tv|/6572E71C6F733714CFF953343/05000900005D8D86FC31C3C3ECEEC1743D0E07-8BDC-49EC-B7E8-672AB3866B19-00011.ts?ccode=0103010103\u0026duration=424\u0026expire=18000\u0026psid=445c0bfac17f247592cdc3645d51b736\u0026ups_client_netip=701c8278\u0026ups_ts=1571283442\u0026ups_userid=\u0026utid=AAFR0caNDtQDAJJ2tU4eAcqA\u0026vid=XMzExNDc3NTQ0MA\u0026s=12efbfbd2defbfbdefbf\u0026sp=282\u0026bc=2\u0026vkey=Ae7cf7eec9c33691dc94ece8edb91c8af\u0026ali_redirect_domain=valipl.cp12.wasu.tv\u0026ali_redirect_ex_ftag=9a6636209503d3127bdf0b659dcc00ca39f95b4dd574afde\u0026ali_redirect_ex_tmining_ts=1571283594\u0026ali_redirect_ex_tmining_expire=3600\u0026ali_redirect_ex_hot=11|okhttp/3.10.0|-|application/octet-stream|200|TCP_MEM_HIT|80|335822|20191017T033955.077Z|20191017T033955.941Z|20191017T033955.078Z|661594430|HIT_RAM|39.134.120.34|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|1|11072|5536|5|197100|860017B6878997039D092684CD7CFF65|27840|http|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|200|ah2-cmcdn5.ahyd.cmcdn.net",
      //       * "source":"/var/log/nginx/static-website.access.log"}
      //       */
      //      val tagsArray = json.getJSONArray("tags")
      //      var city = "未知城市"
      //      if (!tagsArray.isEmpty) {
      //        city = tagsArray.get(0).toString.trim()
      //      }
      //      val messageSplits = json.getString("message").split("\\|")
      //      val timestamp = tranTimeToLong(tranTimeWithhy(messageSplits(0)))
      //      val server_ip = messageSplits(1)
      //      val reqdomain = messageSplits(6)
      //      val filesize = messageSplits(14).toDouble
      //      var upstream_filesize = 0D
      //      if (messageSplits(31).matches("[0-9]+")) {
      //        upstream_filesize = messageSplits(31).toDouble
      //      }
      //      val reqstarttime = tranTimeb2bhy(messageSplits(15)).toDouble
      //      val reqendtime = tranTimeb2bhy(messageSplits(16)).toDouble
      //      var firsttime = 0.0
      //      if (Pattern.matches(".*[a-zA-Z]+.*", messageSplits(17))) {
      //        val responsetime = tranTimeb2bhy(messageSplits(17)).toDouble
      //        firsttime = (responsetime - reqstarttime) / 1000 // 单位换成秒
      //      }
      //      val httpstatus = messageSplits(11)
      //      val upstream_httpstatus = messageSplits(41)
      //      val reqhitstatus = messageSplits(12)
      //
      //      var httpstatus_4xx = 0
      //      var httpststus_5xx = 0
      //      var upstream_4xx = 0
      //      var upstream_5xx = 0
      //      var hit_status = 0
      //      var hit_filesize = 0.0
      //
      //      if (httpstatus.startsWith("5") && upstream_httpstatus.startsWith("5")) {
      //        httpststus_5xx = 1
      //      }
      //      if (httpstatus.startsWith("4") && upstream_httpstatus.startsWith("4")) {
      //        httpstatus_4xx = 1
      //      }
      //      if (upstream_httpstatus.startsWith("4")) {
      //        upstream_4xx = 1
      //      }
      //      if (upstream_httpstatus.startsWith("5")) {
      //        upstream_5xx = 1
      //      }
      //      if (reqhitstatus.contains("HIT")) {
      //        hit_status = 1
      //        hit_filesize = filesize
      //      }
      //      val vendor = Constant.HANG_YAN_KAFKA_TOPIC_NAME
      //      // 转换成自定义对象 => DF
      //      B2BModel(
      //        timestamp,
      //        city,
      //        server_ip,
      //        reqdomain,
      //        filesize,
      //        upstream_filesize,
      //        reqstarttime,
      //        reqendtime,
      //        firsttime,
      //        vendor,
      //        httpstatus_4xx,
      //        httpststus_5xx,
      //        upstream_4xx,
      //        upstream_5xx,
      //        hit_status,
      //        1,
      //        hit_filesize
      //      )
      //    })
      //    val cal: Calendar = Calendar.getInstance()
      //    //调整时间
      //    cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
      //    val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
      //    val path = "data/parquet/" + Constant.HANG_YAN_KAFKA_TOPIC_NAME + "/" + dateStr
      //
      //    hyRDD.coalesce(1).toDF().write.mode(SaveMode.Append).parquet(path)
      /** ******************************************************************************/
      /** ******************************************************************************/
      /** ******************************************************************************/
      /** ******************************************************************************/
      /** ******************************************************************************/
      //      val json = JSON.parseObject(line)
      //      val message = json.getString("message")
      //      val messageSplits = message.split("\\|")
      //      val server_ip = messageSplits(1)
      //      val filesize = messageSplits(14)
      //
      //      messageSplits.length == 46 &&
      //        !"".equals(messageSplits(0)) &&
      //        server_ip.contains(".") &&
      //        filesize.matches("[0-9]+")
      //    }).map(line => { // 取出需要的字段并转换成DataFrame，最后以parquet格式存储
      //      val json = JSON.parseObject(line)
      //      val tagsArray = json.getJSONArray("tags")
      //      var city = "未知城市"
      //      if (!tagsArray.isEmpty) {
      //        city = tagsArray.get(0).toString.trim()
      //      }
      //      val messageSplits = json.getString("message").split("\\|")
      //      val timestamp = tranTimeToLong(messageSplits(0))
      //      val server_ip = messageSplits(1)
      //      var reqdomain = messageSplits(6)
      //      if (reqdomain.contains(":")) {
      //        reqdomain = reqdomain.split(":")(0)
      //      }
      //      val filesize = messageSplits(14).toDouble
      //      var upstream_filesize = 0D
      //      if (messageSplits(31).matches("[0-9]+")) {
      //        upstream_filesize = messageSplits(31).toDouble
      //      }
      //      val reqstarttime = tranTimeb2bfh(messageSplits(15)).toDouble
      //      val reqendtime = tranTimeb2bfh(messageSplits(16)).toDouble
      //      var firsttime = 0.0
      //      if (!"null".equalsIgnoreCase(messageSplits(17))) {
      //        val responsetime = tranTimeb2bfh(messageSplits(17)).toDouble
      //        firsttime = (responsetime - reqstarttime) / 1000 // 单位换成秒
      //      }
      //      val httpstatus = messageSplits(11)
      //      val upstream_httpstatus = messageSplits(41)
      //      val reqhitstatus = messageSplits(12)
      //
      //      var httpstatus_4xx = 0
      //      var httpststus_5xx = 0
      //      var upstream_4xx = 0
      //      var upstream_5xx = 0
      //      var hit_status = 0
      //      var hit_filesize = 0.0
      //
      //      if (httpstatus.startsWith("5") && upstream_httpstatus.startsWith("5")) {
      //        httpststus_5xx = 1
      //      }
      //      if (httpstatus.startsWith("4") && upstream_httpstatus.startsWith("4")) {
      //        httpstatus_4xx = 1
      //      }
      //      if (upstream_httpstatus.startsWith("4")) {
      //        upstream_4xx = 1
      //      }
      //      if (upstream_httpstatus.startsWith("5")) {
      //        upstream_5xx = 1
      //      }
      //      if (reqhitstatus.contains("HIT")) {
      //        hit_status = 1
      //        hit_filesize = filesize
      //      }
      //      val vendor = Constant.FONSVIEW_KAFKA_B2B_TOPIC_NAME
      //      // 转换成自定义对象 => DF
      //      B2BModel(
      //        timestamp,
      //        city,
      //        server_ip,
      //        reqdomain,
      //        filesize,
      //        upstream_filesize,
      //        reqstarttime,
      //        reqendtime,
      //        firsttime,
      //        vendor,
      //        httpstatus_4xx,
      //        httpststus_5xx,
      //        upstream_4xx,
      //        upstream_5xx,
      //        hit_status,
      //        1,
      //        hit_filesize
      //      )
      //    })
      //    val cal: Calendar = Calendar.getInstance()
      //    //调整时间
      //    cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
      //    val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
      //    val path = "data/parquet/" + Constant.FONSVIEW_KAFKA_B2B_TOPIC_NAME + "/" + dateStr
      //
      //    hyRDD.coalesce(1).toDF().write.mode(SaveMode.Append).parquet(path)
      /** ******************************************************************************/
      /** ******************************************************************************/
      /** ******************************************************************************/
      /** ******************************************************************************/
      /** ******************************************************************************/
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
      flag && messageSplits(0).trim.contains(".") && Pattern.matches(".*\\.(com|cn)", messageSplits(1).trim)
    }).map(line => { // 取出需要的字段并转换成DataFrame，最后以parquet格式存储
      val json = JSON.parseObject(line)
      /**
       *
       */
      val tagsArray = json.getJSONArray("tags")
      var city = "未知城市"
      if (!tagsArray.isEmpty) {
        city = tagsArray.get(0).toString.trim()
      }
      val messageSplits = json.getString("message").split("\"")
      val fisrttimes = messageSplits(2).trim.split(" ")
      val server_ip = messageSplits(0).trim
      val reqdomain = messageSplits(1).trim
      val firsttime = fisrttimes(0).toDouble
      val timestamp = tranTimeToLong(tranTimeZte(fisrttimes(1).drop(1)))
      val httpstatus_filesize = messageSplits(4).trim.split(" ")
      val httpstatus = httpstatus_filesize(0).trim
      val filesize = httpstatus_filesize(1).trim.toDouble
      val sendtime = messageSplits(9).trim.toDouble // 单位秒
      val reqstarttime = timestamp
      val reqendtime = (timestamp + sendtime * 1000).toLong // tranTimeToLong 返回的是毫秒 所以要乘1000
      val upstream_cache_status = messageSplits(11).trim
      val upstream_httpstatus = messageSplits(12).trim

      val upstream_filesize = 0.0 // 源数据中无，故全部置零
      // 速率
      var res = 0.0
      if (reqendtime - reqstarttime != 0) {
        res = (filesize / 1024 / 1024) / (reqendtime - reqstarttime) * 1000
      }
      var httpstatus_4xx = 0
      var httpststus_5xx = 0
      var upstream_4xx = 0
      var upstream_5xx = 0
      var hit_status = 0
      var hit_filesize = 0.0

      if (httpstatus.startsWith("5") && upstream_httpstatus.startsWith("5")) {
        httpststus_5xx = 1
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
      if (upstream_cache_status.contains("HIT")) {
        hit_status = 1
        hit_filesize = filesize
      }
      val vendor = Constant.ZTE_KAFKA_B2B_TOPIC_NAME
      // 转换成自定义对象 => DF
      B2BModel(
        timestamp,
        city,
        server_ip,
        reqdomain,
        filesize,
        upstream_filesize,
        reqstarttime,
        reqendtime,
        res,
        0,
        firsttime,
        vendor,
        httpstatus_4xx,
        httpststus_5xx,
        upstream_4xx,
        upstream_5xx,
        hit_status,
        1,
        hit_filesize
      )
    })
    val cal: Calendar = Calendar.getInstance()
    //调整时间
    cal.add(Calendar.HOUR, 0) //或是Calendar.HOUR_OF_DAY
    val dateStr = new SimpleDateFormat("yyyy-MM-dd-HH").format(cal.getTime())
    val path = "data/parquet/" + Constant.ZTE_KAFKA_B2B_TOPIC_NAME + "/" + dateStr

    hyRDD.coalesce(1).toDF().write.mode(SaveMode.Append).parquet(path)
  }

  /**
   * 初始化spark配置
   *  conf.setMaster("local")
   */
  override def init(): Unit = {
    conf.setMaster("local[4]")
  }
}
