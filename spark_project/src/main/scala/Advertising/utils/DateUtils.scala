package Advertising.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间工具类
 */
object DateUtils {
	
	/**
	 * 获取当前的日期，格式为:20190710
	 */
	def getTodayDate(): String = {
		// a. 获取当前日期
		val nowDate = new Date()
		// b. 转换日期格式
		val date: Date = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm").parse("2013-12-01T00:00")
		FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(date)
	}

	def main(args: Array[String]): Unit = {
		println(getTodayDate())
	}
	
	
	/**
	 * 获取昨日的日期，格式为:20190710
	 */
	def getYesterdayDate(): String = {
		// a. 获取Calendar对象
		val calendar: Calendar = Calendar.getInstance()
		// b. 获取昨日日期
		calendar.add(Calendar.DATE, -1)
		// c. 转换日期格式
		FastDateFormat.getInstance("yyyy-MM-dd").format(calendar)
	}
	
}
