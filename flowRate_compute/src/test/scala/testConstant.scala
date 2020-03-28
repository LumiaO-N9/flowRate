import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Date, TimeZone}

import fun.lumia.ComputeConstant

object testConstant extends App {
  val nowDate = LocalDate.now()
  println(nowDate)
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
  val cal: Calendar = Calendar.getInstance()
  println(cal.getTime())
  val currentTime = cal.getTime()
  println(currentTime)
  val currentHour = dateFormat.format(currentTime)
  println(currentHour)
  cal.add(Calendar.HOUR, -1) //或是Calendar.HOUR_OF_DAY
  val frontHour = dateFormat.format(cal.getTime())
  println(frontHour)
  val now1 = new Date().getTime()
  println(now1)
  val currentHourPath = dateFormat.format(now1)
  println(currentHourPath)
  val frontOneHourTime = now1 - 3600 * 1000
  val frontOneHourPath = dateFormat.format(frontOneHourTime)
  println(frontOneHourPath)
  //yyyy/MM/dd HH:mm:ss
  val dateMinuteFormate: SimpleDateFormat = new SimpleDateFormat("mm")
  val minute = dateMinuteFormate.format(now1)
  println(minute)
  val max = 20000000000L
  val min = 10000000000L
  val filterSqlStr = "select * from DFTable where ( reqstarttime>" + min + " and reqstarttime<" + max + ") or (reqendtime>" + min + " and reqendtime<" + max + ") or (reqstarttime<" + min + " and reqendtime>" + max + ")"
  println(filterSqlStr)


  val currentTime2 = new Date().getTime()
  println(currentTime2)
  val secondFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  val currentTimeSecond = secondFormat.parse(secondFormat.format(currentTime2)).getTime()
  println(currentTimeSecond)
  val filterSqlStr2 = "select * from DFTable where " + currentTimeSecond + ">reqstarttime and reqendtime>" + currentTimeSecond
  println(filterSqlStr2)
  val defaultRes = 1.2
  val filterSqlStr3 = "select * from DFTable where res<=" + defaultRes + " and (( reqstarttime>=" + min + " and reqstarttime<=" + max + ") or (reqendtime>=" + min + " and reqendtime<=" + max + ") or (reqstarttime<=" + min + " and reqendtime>=" + max + "))"
  println(filterSqlStr3)
  val strZone = "2019-12-14-12 +08"
  val formatZone = new SimpleDateFormat("yyyy-MM-dd-HH")
  formatZone.setTimeZone(TimeZone.getTimeZone("UTC"))
  println(formatZone.parse(strZone))

  import java.text.SimpleDateFormat
  import java.util.TimeZone

  val utcTime = "2018-01-31T14:32:19Z"
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  //设置时区UTC
//  df.setTimeZone(TimeZone.getTimeZone("UTC"))
  //格式化，转当地时区时间
  val after = df.parse(utcTime)
  println(after)
}
