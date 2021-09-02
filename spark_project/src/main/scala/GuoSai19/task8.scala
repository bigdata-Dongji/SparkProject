package GuoSai19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object task8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task8")
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
//    frame.na.replace("省份",
//      Map(
//        Array("山东","江苏","安徽","浙江","江西","福建","上海") -> "华东地区",
//        Array("广东","广西","海南") -> "华南地区",
//        Array("湖北","湖南","河南") -> "华中地区",
//        Array("北京","天津","河北","山西","内蒙古") -> "华北地区",
//        Array("宁夏","新疆","青海","陕西","甘肃") -> "西北地区",
//        Array("四川","云南","贵州","西藏","重庆") -> "西南地区",
//        Array("辽宁","吉林","黑龙江") -> "东北地区")
//    )
    val frame1 = frame.select('SEQ,
      when('省份.isin("山东", "江苏", "安徽", "浙江", "江西", "福建", "上海"), "华东地区")
        .when('省份.isin("广东", "广西", "海南"), "华南地区")
        .when('省份.isin("湖北", "湖南", "河南"), "华中地区")
        .when('省份.isin("北京", "天津", "河北", "山西", "内蒙古"), "华北地区")
        .when('省份.isin("宁夏", "新疆", "青海", "陕西", "甘肃"), "西北地区")
        .when('省份.isin("四川", "云南", "贵州", "西藏", "重庆"), "西南地区")
        .when('省份.isin("辽宁", "吉林", "黑龙江"), "东北地区") as ("省份")
    )
    val frame3 = frame.drop("省份")
    val frame2 = frame3.join(frame1,"SEQ")
//    frame2.show()
    //求地区、总订单、总间夜、实住订单、实住间夜、出租率
    frame2.groupBy("省份").agg(
      sum('酒店总订单.cast(DoubleType)).as("总订单"),
      sum('酒店总间夜.cast(DoubleType)).as("总间夜"),
      sum('酒店实住订单.cast(DoubleType)).as("实住订单"),
      sum('酒店实住间夜.cast(DoubleType)).as("实住间夜"),
      sum('酒店总间夜.cast(DoubleType)/'房间数.cast(DoubleType)).as("出租率")
    ).show()

  }
}
