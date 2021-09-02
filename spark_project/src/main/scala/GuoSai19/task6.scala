package GuoSai19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object task6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task6")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("header", true)
      //      .option("inferSchema", true)
      .csv("datas/hoteldata3.csv/part-r-00000-8b8d7b64-0242-4496-8c02-c6c7a49026cc.csv")
    //    frame.printSchema()
    frame.show()
    frame.createOrReplaceTempView("rawdata")

    //统计各城市的酒店数量和房间数量，以城市房间数量降序排列，并打印输出前 10 条统计结果
    spark.sql(
      """
        |select `省份`,`城市`,count(`酒店`),sum(cast(`房间数` as int)) num
        |from rawdata
        |group by `省份`,`城市`
        |order by num desc limit 10
        |""".stripMargin).show()

    frame.groupBy("省份","城市")
      .agg(count("酒店").as("酒店数量"),
        sum('房间数.cast(IntegerType)).as("房间数量"))
      .orderBy('房间数量.desc)
      .limit(10).show()

  }
}
