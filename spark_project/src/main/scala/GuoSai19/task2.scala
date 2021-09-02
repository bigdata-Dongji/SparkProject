package GuoSai19

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

object task2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task2")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("encoding","GBK")
      .option("header", true)
      //      .option("inferSchema", true)
      .csv("datas/hotel_data.csv")
//    frame.show() //13034

    //将关键字段{星级、评论数、评分}中任意字段为空的数据删除；

    val unit: Dataset[Row] = frame.filter('星级==="NULL" or '评分==="NULL" or '评论数==="NULL")
    println("删除的记录数为："+unit.count())//7148

  }
}
