package GuoSai19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

object task3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task3")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("header", true)
      //      .option("inferSchema", true)
      .csv("datas/hoteldata1.csv/part-r-00000-d97df015-3ec0-4371-b83f-9181634c2f4d.csv")
//    frame.show()
    /*
    剔除数据集中评分和星级字段的非法数据，合法数据是评分[0，5]的实数，
    星级是指星级字段内容中包含 NULL、二星、三星、四星、五星的数据；
    剔除数据集中的重复数据；
     */
    val unit = frame.select('*).where('评分.cast(DoubleType)>=0 and '评分.cast(DoubleType)<=5)
    println("评分剔除的数据为："+(frame.count()-unit.count()))//108

//    frame.groupBy("星级").agg(count("星级")).show()
    val unit1 = frame.select('*).where('星级.substr(0,2).isin("二星","三星","四星","五星"))
    println("星级剔除的数据为："+(frame.count()-unit1.count()))//135

    val unit2 = frame.dropDuplicates()
//    println(frame.count())//3304
    println("去重剔除的数据为："+(frame.count()-unit2.count()))//0

    val unit3 = unit.select('*).where('星级.substr(0,2).isin("二星","三星","四星","五星"))
    unit3.write.option("header",true).csv("datas/hoteldata3.csv")
  }
}
