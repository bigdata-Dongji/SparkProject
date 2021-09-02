package GuoSai19

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object task4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task4")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("header", true)
      //      .option("inferSchema", true)
      .csv("datas/hoteldata3.csv/part-r-00000-8b8d7b64-0242-4496-8c02-c6c7a49026cc.csv")
//    frame.show()

    //计算城市总订单、城市酒店平均用户评分、城市酒店总评论数
    val frame1 = frame.groupBy("城市").agg(
      sum('酒店总订单.cast(IntegerType)).as("城市总订单"),
      avg('评分.cast(DoubleType)).as("城市酒店平均用户评分"),
      sum('评论数.cast(IntegerType)).as("城市酒店总评论数")
    )
    frame1.show()
//    frame1.select('*).where('城市.isin("北京","上海","广州")).show()

    //打印输出城市总订单、城市酒店平均用户评分、城市酒店总评论数三个指标的最大值和最小值
    println("---------------------城市总订单指标的最大值和最小值----------------------------")
    frame1.select(min("城市总订单"),max("城市总订单")).show()
    println("---------------------城市酒店平均用户评分指标的最大值和最小值----------------------------")
    frame1.select(min("城市酒店平均用户评分"),max("城市酒店平均用户评分")).show()
    println("---------------------城市酒店总评论数指标的最大值和最小值----------------------------")
    frame1.select(min("城市酒店总评论数"),max("城市酒店总评论数")).show()

    //将城市总订单、城市酒店平均用户评分、城市酒店总评论数进行归一化处理
    //(x - Min) / (Max - Min)
    val commentmax = frame1.groupBy().max("城市酒店总评论数").first().toSeq(0).toString.toInt
    val commentmin = frame1.groupBy().min("城市酒店总评论数").first().toSeq(0).toString.toInt

    val ordermax = frame1.groupBy().max("城市总订单").first().toSeq(0).toString.toInt
    val ordermin = frame1.groupBy().min("城市总订单").first().toSeq(0).toString.toInt

    val scoremax = frame1.groupBy().max("城市酒店平均用户评分").first().toSeq(0).toString.toDouble
    val scoremin = frame1.groupBy().min("城市酒店平均用户评分").first().toSeq(0).toString.toDouble

    val result: DataFrame =frame1
      .withColumn("城市酒店总评论数归一化结果", ('城市酒店总评论数-commentmin)/(commentmax-commentmin))
      .withColumn("城市总订单归一化结果", ('城市总订单-ordermin)/(ordermax-ordermin))
      .withColumn("城市酒店平均用户评分归一化结果", ('城市酒店平均用户评分-scoremin)/(scoremax-scoremin))
    result.show()
    result.coalesce(1).write.option("header",true).csv("datas/hoteldata4.csv")
  }
}
