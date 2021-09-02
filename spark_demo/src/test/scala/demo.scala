import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat

object demo {
  def main(args: Array[String]): Unit = {
    val date = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm").parse("2013-12-01T00:00")
    val str = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(date)
    println(str)
  }
}
