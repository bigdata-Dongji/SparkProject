package JournalQx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object journal_event {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jour")
      .master("local[6]")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //导入隐式转换
    import spark.implicits._
    //读取数据
    val eventsdf = spark.read.option("header",true).csv("G:\\QQ\\2020省赛\\QX\\events\\events.csv")
    val userdf = spark.read.option("header",true).csv("G:\\QQ\\2020省赛\\QX\\events\\users.csv")
    eventsdf.printSchema()
    eventsdf.show()
//    TODO 需求1：查询数据总量（不重复）
//    println("数据总行数："+eventsdf.count())  // 3137972
//    println("event_id总行数"+eventsdf.select('event_id).distinct().count())  // 3137972

//    TODO 需求2：查询事件表中有没有用户id与用户表用户id一致
    eventsdf.createOrReplaceTempView("events")
    userdf.createOrReplaceTempView("user")
//    eventsdf.groupBy('user_id )
//      .agg(count('user_id).as("count_id"))
//      .orderBy('count_id.desc)
//      .limit(10)
//      .show()
    spark.sql(
      """
        |select e.user_id,count(e.user_id)
        |from events e join user u
        |on e.user_id = u.user_id
        |group by e.user_id
        |""".stripMargin).show()

//    TODO 需求3：查询有没有无效的 start_time 的时间
    /*
    时间格式(标准的) 2020-01-13T16:00:00.000Z    （残缺的，修改格式）
    T表示分隔符，Z表示的是UTC。
    UTC：世界标准时间，在标准时间上加上8小时，即东八区时间，也就是北京时间。
    举例 北京时间：2020-01-14 00:00:00对应的国际标准时间格式为：2020-01-13T16:00:00.000Z
     */
//    eventsdf.select('start_time.cast(DateType)).show()  只取日期，丢弃时分秒等
//    eventsdf.select('start_time.cast(TimestampType)).show() //标准的
//    eventsdf.select(date_format('start_time,"yyyy-MM-dd'T'HH:mm:ss.SSS Z")).show() // 反过来可以封装成上面的数据
//    eventsdf.select(date_format('start_time.cast(TimestampType),"yyyy-MM-dd HH:mm:ss")).show()
//    regexp_replace('start_time,"Z"," UTC")  // java中SimpleDateFormat需要替换，scala不用，TimestampType 直接转换功能强大
    val eventdf = eventsdf.withColumn("start_time",date_format('start_time.cast(TimestampType),"yyyy-MM-dd HH:mm:ss"))
//    eventdf.show()
//    eventdf.printSchema()
    // ^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.* 用来查询 2020-01-13T16:00:00.000Z 这种格式的
    // rlike 是 like 的反义
    println(eventdf.filter('start_time.rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")).count())  //3137972
  }
}
