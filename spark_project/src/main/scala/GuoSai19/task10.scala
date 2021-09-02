package GuoSai19

import org.apache.spark.sql.SparkSession

object task10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task10")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("datas/hoteldata4.csv/part-r-00000-02769830-aac5-459d-a6fc-fc0eaafb7958.csv")
    //    frame.printSchema()
    frame.show()
    frame.createOrReplaceTempView("rawdata")

    /*
    从城市的酒店总订单、用户评分及评论数角度综合分析并
    获得城市的受欢迎程度排名，取最受游客欢迎的 5 个城市
    权重分配说明：归一化城市酒店总订单 0.6，归一化用户评分0.2， 归一化评论数 0.2。
     */
    frame.select('*)
      .where('城市总订单归一化结果>=0.6 and '城市酒店总评论数归一化结果>=0.2 and '城市酒店平均用户评分归一化结果>=0.2)
      .show()
  }
}
