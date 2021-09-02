package Task

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.types.DateType

object T12_17 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("12-17")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val df: DataFrame =spark.read.option("header",true)
      .option("inferSchema",true)
      .csv("G:\\QQ\\2020省赛\\综合测评3-1216\\新冠肺炎疫情数据\\城市疫情.csv")
//    df.show(10)
//    df.printSchema()
    //重复数据47条
//    println(df.count())
//    println(df.distinct().count())
    val newdf: Dataset[Row] = df.dropDuplicates()
    //修改时间列
    val frame = newdf.withColumn("日期",regexp_replace('日期,"/","-").cast(DateType))
//    println(frame.count())
//    frame.printSchema()
    val value = frame.select('*).where('日期>"2020-01-10"&&'日期<"2020-06-30")
  .groupBy("城市").agg(
      sum("新增确诊").as("累计确诊人数"),
      sum("新增治愈").as("累计治愈人数"),
      sum("新增死亡").as("累计死亡人数")
    )
    value.show()
//    value.coales  ce(1).write.csv("datas/task1_1")
//    newdf.show()
//    newdf.select(dayofmonth())
    spark.stop()
  }
}
