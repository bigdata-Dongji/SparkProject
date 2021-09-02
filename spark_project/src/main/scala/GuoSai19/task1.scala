package GuoSai19

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object task1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task1")
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
//    frame.show()
//    frame.printSchema()

    //将缺失值大于 n（n=3）个的数据条目剔除出原始数据集，并输出剔除的条目数量；
    val unit: RDD[Row] = frame.rdd
      .map( x => x.toString() )
      .map( x => x.split(","))
      .filter( x => x.count(_ == "NULL") <= 3)
      .map( item => item.toSeq)
      .map( line => Row.fromSeq(line) )
//    val num = frame.rdd
//      .map(x => x.toString())
//      .map(_.split(","))
//      .filter(_.count(_ == "NULL") > 3).count()
//    println("剔除的条目数量:"+num)//9730
    val frame1 = spark.createDataFrame(unit,frame.schema)
    frame1.show()
//    frame1.write.option("header",true).csv("datas/hoteldata1.csv")

    //另一种方式
//      .map { line: String => line.split(",") }
//      .filter { words: Array[String] => words.count(_ == "NULL") > 3 }
  }
}
