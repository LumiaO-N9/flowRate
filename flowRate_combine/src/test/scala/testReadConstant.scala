import java.util.regex.Pattern

object testReadConstant extends App {
  println()
  val reqendtime = "20191223 15:00:15.269"
  val reqstarttime = "20191223 15:20:22.261"
  val regexStr = "\\d{8}\\s{1}\\d{2}:\\d{2}:\\d{2}\\.\\d{3}"
  println(Pattern.matches(regexStr, reqstarttime) && Pattern.matches(regexStr, reqendtime))

}
