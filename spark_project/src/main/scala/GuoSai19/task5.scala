package GuoSai19

import org.apache.spark.sql.SparkSession

object task5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task5")
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
    //查看 MySQL 的 hoteldata 数据库中 rawdata 表的总行数
    spark.sql(
      """
        |select count(*) from rawdata
        |""".stripMargin).show()

  }
}
