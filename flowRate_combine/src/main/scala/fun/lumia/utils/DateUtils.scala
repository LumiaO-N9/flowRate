package fun.lumia.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}


object DateUtils {

  val tranTimeToLong: (String) => Long = (timestamp: String) => {
    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    fm.parse(timestamp).getTime
  }
  val tranTimeToLongFormat: (String) => Long = (timestamp: String) => {
    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val fm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:00")

    fm.parse(fm2.format(fm.parse(timestamp))).getTime


  }

  def timeToGmt(timestamp: String): Long = {
    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val fromTimeZone = "GMT+8"
    fm.setTimeZone(TimeZone.getTimeZone(fromTimeZone))
    fm.parse(timestamp).getTime
  }

  /**
   * Long类型时间戳转字符串
   *
   * @param timestamp
   * @return
   */
  def tranTimeToString(timestamp: Long): String = {
    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val d = new Date(timestamp)
    fm.format(d)
  }

  /**
   * hy时间戳转换
   *
   * @param timestamp
   * @return
   */
  def tranTimeWithTZ(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    dm2.format(time)
  }

  /**
   * 转换普罗米修斯时间
   */
  def tranTimeWithPu(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    fm1.parse(timestamp).getTime
  }

  /**
   * hyB2B时间戳含毫秒
   */
  def tranTimeWithZs(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss.sss'Z'")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    dm2.format(time)
  }


  def tranTime(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    dm2.format(time)
  }

  def tranTimeZte(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("CH"))
    //    val fromTimeZone = "GMT+0"
    //    fm1.setTimeZone(TimeZone.getTimeZone(fromTimeZone))
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    //    val toTimeZone = "GMT+8"
    //    dm2.setTimeZone(TimeZone.getTimeZone(toTimeZone))
    val time = fm1.parse(timestamp).getTime
    dm2.format(time)
  }

  def tranTimeProbe(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("yyyy-MM-dd HHmmss")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    dm2.format(time)
  }


  /**
   * 转换中兴ott时间
   */
  def tranTimeOttZte(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    dm2.format(time)
  }


  /**
   * @卜宏亮，，转换hyb2b时间,含毫秒
   */
  def tranTimeb2bhy(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss SSS")
    val time = fm1.parse(timestamp)
    dm2.parse(dm2.format(time)).getTime + 8 * 60 * 60 * 1000
  }

  /**
   * @卜宏亮 ，转换fhb2b时间，含毫秒
   * @param timestamp
   * @return
   */
  def tranTimeb2bfh(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss SSS")
    val time = fm1.parse(timestamp)
    dm2.parse(dm2.format(time)).getTime
  }

  /**
   * @卜宏亮，，，转换杭研b2b时间
   */
  def tranTimeWithhy(timestamp: String): String = {
    val fm1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss")
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    dm2.format(time)
  }

  /**
   * fhott时间
   */
  def tranTimeOttFh(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyyMMddHHmmss")
    fm1.parse(timestamp).getTime
  }

  val tranTimeToLongOttHW: (String) => Long = (timestamp: String) => {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:sss")
    fm.parse(timestamp).getTime
  }

  def isNullOrEmptyAfterTrim(string: String): Boolean = string == null || string.trim == "" || string.trim.toUpperCase == "NIL" || string.trim.toUpperCase == "NULL" || string.trim == "-"

  def formatTime(timestamp: Long): Date = {
    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:00")
    dm2.parse(dm2.format(timestamp))
  }

  def formatFhStartLong(starttime: Long): Long = {
    val time = formatTime(starttime)
    val dm3 = new SimpleDateFormat("mm")
    time.getTime - dm3.format(time).toLong % 5 * 60 * 1000
  }

  def formatFhEndLong(endtime: Long): Long = {
    val dm3 = new SimpleDateFormat("mm")
    val time = formatTime(endtime)
    time.getTime + (5 - dm3.format(time).toLong % 5) * 60 * 1000
  }

  def formatTimeTemp = (timeStampTemp: Long) => {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(timeStampTemp + 5 * 60 * 1000)
  }

  def tranTimeOttfh(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS")
    //    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss SSS")
    val time = fm1.parse(timestamp)
    //    dm2.parse(dm2.format(time)).getTime + 8 * 60 * 60 * 1000
    //    time.getTime + 8 * 60 * 60 * 1000
    time.getTime
  }

  def tranTimeOttHW(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    //    val fm1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("CH"))
    //    val dm2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val time = fm1.parse(timestamp)
    time.getTime + 8 * 60 * 60 * 1000
  }

  def tranTimeOttzte(timestamp: String): Long = {
    val fm1 = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
    fm1.parse(timestamp).getTime
  }


}
