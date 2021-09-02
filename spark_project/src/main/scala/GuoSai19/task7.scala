package GuoSai19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object task7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task7")
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

    //统计各省直销拒单率，以直销拒单率升序排列并输出前 10 条统计结果
    spark.sql(
      """
        |select `省份`,sum(cast(regexp_replace(`酒店直销拒单率`,'%','') as double)/100.0) as num
        |from rawdata
        |group by `省份` order by num limit 10
        |""".stripMargin).show()

    frame.groupBy("省份").agg(sum(
      regexp_replace('酒店直销拒单率,"%","").cast(DoubleType)/100.0
    ).as("省直销拒单率")).orderBy('省直销拒单率.asc).limit(10).show()

  }
}
