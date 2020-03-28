import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar

import fun.lumia.Constant

object testConstant extends App {
  println(Constant.KAFKA_BOOTSTRAP_SERVERS)
  val str = "|a|b|c"
  val strSplits = str.split("\\|")
  println(strSplits(0) == "")
  val nowDate = LocalDate.now()
  println(nowDate)
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
  val cal: Calendar = Calendar.getInstance()
  println(cal.getTime())
  cal.add(Calendar.HOUR, 15) //或是Calendar.HOUR_OF_DAY
  val frontHour = dateFormat.format(cal.getTime())
  println(frontHour)
  val tranTimeToLong: (String) => Long = (timestamp: String) => {
    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    fm.parse(timestamp).getTime
  }
  val time1 = tranTimeToLong("2019/11/06 14:32:30")
  println(time1)

  val a = 0.0
  if (0.0 == 0) {
    println("a != 0")
  }

//  println("".toDouble)

}
