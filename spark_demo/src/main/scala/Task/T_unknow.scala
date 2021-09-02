package Task

import java.util.{Calendar, Properties}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/*
去掉字段“上映天数”中带有“零点场”、“点映”、“展映”和“重映”的电影数据；
以字段“上映天数”和“当前日期”为依据，在尾列添加一个“上映日期”（releaseDate）的字段，
该字段值为“当前日期”减去“上映天数”+1（格式为：2020-10-13）。
例如：若字段“上映天数”的值为“上映 2 天”，字段“当前日期”为“2020-10-10”，
则字段“上映日期”的值为“2020-10-09”。如果字段“上映天数”为空，则字段“上映日期”
的值设为“往期电影”。注意：若字段“上映天数”的值为“上映首日”，则字段“上映日期”的值应设为“当前日期”的值；

对字段“当日综合票房”和字段“当前总票房”的数据值进行处理，单位量级统
一以“万”展示数据，若原数据中有“万”等表示量级的汉字，需去掉量级汉字。如：
“1.5 亿”需转换为“15000”，“162.4 万”转换为“162.4”，转换时需注意精度缺失问题，
当我们使用 double、float 等类型做算术运算时，例如：1.14 亿转换为万时，结
果会出现 11399.999999999998 万，这里我们可以使用 BigDecimal 类来处理这类
精度缺失问题，字段“当日综合票房”和字段“当前总票房”最后保留两位小数；
 */
object T_unknow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("T_unknow")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("G:\\QQ\\2020省赛\\测评1\\综合测评1\\cleandata.csv")
//    frame.printSchema()
//    frame.show()
    //去掉字段“上映天数”中带有“零点场”、“点映”、“展映”和“重映”的电影数据；
    val unit = frame.select('*)
      .where(!substring('movie_days,0,2).isin("零点","点映","展映","重映"))
    unit.show()
    /*
    以字段“上映天数”和“当前日期”为依据，在尾列添加一个“上映日期”（releaseDate）的字段，
    该字段值为“当前日期”减去“上映天数”+1（格式为：2020-10-13）。
    例如：若字段“上映天数”的值为“上映 2 天”，字段“当前日期”为“2020-10-10”，
    则字段“上映日期”的值为“2020-10-09”。如果字段“上映天数”为空，则字段“上映日期”
    的值设为“往期电影”。注意：若字段“上映天数”的值为“上映首日”，则字段“上映日期”的值应设为“当前日期”的值；
     */
    val frame1 = unit.withColumn("current_time",regexp_replace('current_time,"/","-"))
    val frame2 = frame1.withColumn("releaseDates",
      when(regexp_replace('movie_days, "上映|天", "") === "", "往期电影")
        .when('movie_days === "上映首日", 'current_time)
        .when(regexp_replace('movie_days, "上映|天", "").cast(IntegerType) + 1 > 1, regexp_replace('movie_days, "上映|天", ""))
    )
    frame2.createOrReplaceTempView("frame")
    val frame3 = spark.sql(
      """
        |select *,
        |case when cast(releaseDates as Int)+1>1 then date_sub(current_time,cast(releaseDates as Int)) else releaseDates end as releaseDate
        |from frame
        |""".stripMargin)
    val frame4 = frame3.drop("releaseDates")
    frame4.show()
  /*
  对字段“当日综合票房”和字段“当前总票房”的数据值进行处理，单位量级统
  一以“万”展示数据，若原数据中有“万”等表示量级的汉字，需去掉量级汉字。如：
  “1.5 亿”需转换为“15000”，“162.4 万”转换为“162.4”，转换时需注意精度缺失问题，
  当我们使用 double、float 等类型做算术运算时，例如：1.14 亿转换为万时，结
  果会出现 11399.999999999998 万，这里我们可以使用 BigDecimal 类来处理这类
  精度缺失问题，字段“当日综合票房”和字段“当前总票房”最后保留两位小数；
   */
    val frame5 = frame4.withColumn("boxoffice",
      when('boxoffice.contains("万"), regexp_replace('boxoffice, "万", ""))
        .when('boxoffice.contains("亿"), round(regexp_replace('boxoffice, "亿", "").cast(DoubleType) * 10000.0,2))
    ).withColumn("total_boxoffice",
      when('total_boxoffice.contains("万"), regexp_replace('total_boxoffice, "万", ""))
        .when('total_boxoffice.contains("亿"), round(regexp_replace('total_boxoffice, "亿", "").cast(DoubleType) * 10000.0,2))
    )
    frame5.show()
//    frame5.write.mode(SaveMode.Overwrite)
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("user","root")
//      .option("password","511722")
//      .jdbc("jdbc:mysql://localhost:3306","advertising.datas",new Properties())
  }
}
