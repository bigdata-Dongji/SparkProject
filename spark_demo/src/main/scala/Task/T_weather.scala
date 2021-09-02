package Task

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object T_weather {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("T_weather")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame: RDD[String] = spark.sparkContext.textFile("E:\\数据文件\\meteorologicaldata\\a.txt")
    /*
     清除不合法数据：字段长度不足，风向不在[0,360]的，风速为负的，气压为负的，天气情况不在[0,10]，
     湿度不在[0,100]，温度不在[-40,50]的数据；分隔符为逗号
     */
    val unit: RDD[Row] = frame.map((line: String) => line.split("\\s+"))
      .filter((line: Array[String]) => line.length == 12)
      .filter((line: Array[String]) => line(7).toInt >= 0 && line(7).toInt <= 360)
      .filter((line: Array[String]) => line(6).toInt >= 0)
      .filter((line: Array[String]) => line(8).toInt >= 0)
      .filter((line: Array[String]) => line(9).toInt >= 0 && line(9).toInt <= 10)
      .filter((line: Array[String]) => line(5).toInt >= 0 && line(5).toInt <= 100)
      .filter((line: Array[String]) => line(4).toInt >= -40 && line(4).toInt <= 50)
        .map(line=> Row.fromSeq(line.toSeq))
    /*
    将a.txt与sky.txt的数据以天气情况进行join操作，把天气情况变为其对应的云属；
    对进入同一个分区的数据排序； 排序规则： （1）同年同月同天为key； （2）按每日温度升序；
    （3）若温度相同则按风速升序； （4）风速相同则按压强降序。
   */

    val schema=StructType(
      List(
        StructField("year",StringType),
        StructField("month",StringType),
        StructField("day",StringType),
        StructField("hour",StringType),
        StructField("temperature",StringType),
        StructField("humidity",StringType),
        StructField("pressure",StringType),
        StructField("winddirection",StringType),
        StructField("windspeed",StringType),
        StructField("weather",StringType),
        StructField("1h_rainfall",StringType),
        StructField("6h_rainfall",StringType)
      )
    )
    val frame1 = spark.createDataFrame(unit,schema)
//    frame1.show()
    val skyschema=StructType(
      List(
        StructField("weather",StringType),
        StructField("weather_name",StringType)
      )
    )
    val frame2 = spark.read.schema(skyschema).csv("E:\\数据文件\\meteorologicaldata\\sky.txt")
//    frame2.show()
    val frame3 = frame1.join(frame2,"weather")
    frame3.show()
    frame3.createOrReplaceTempView("frame3")
//    frame3.select('*).orderBy('temperature.cast(IntegerType).asc, 'windspeed.cast(IntegerType).asc, 'pressure.cast(IntegerType).desc)
//      .groupBy('year, 'month, 'day)
//    spark.sql(
//      """
//        |select year,month,day,temperature,windspeed,pressure from frame3
//        |group by year,month,day
//        |order by temperature asc,windspeed asc,pressure desc
//        |""".stripMargin).show()
  }
}
