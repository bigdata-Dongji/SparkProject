package Task

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object T12_22 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("1222")
      .master("local")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
        .option("encoding","GBK")
      .option("header", true)
      .csv("G:\\QQ\\2020省赛\\综合测评4-1222\\附件.csv")
//    frame.show()
    //去除数据中带 ***** 2条
    val unit = frame.filter('小类名称=!="*****")
//    println(unit.count())   //42814
    //去除销售金额，销售数量，商品单价的数据为负数的
    val unit1 = unit.filter('销售金额.cast(IntegerType)>0&&'销售数量.cast(IntegerType)>0&&'商品单价.cast(IntegerType)>0)
//    println(unit1.count())    //32841
    //去除单位为数字的数据
    val unit2 = unit1.filter(!'单位.rlike("\\d+"))
    //商品规格缺失值采用众数填充   众数为散称
    val rdd = unit2.groupBy("规格型号").agg(count("规格型号").as("num")).orderBy('num.desc).limit(1).rdd
      .map(line => line.toSeq(0))

//    val frame1 = unit2.na.replace("规格型号",Map(" "->"散称"))
    //单位从规格型号中取，散称为g
//    val frame2 = frame1.withColumn("单位",
//      when('规格型号.contains("g"), "g")
//        .when('规格型号.contains("散称"), "g").otherwise('单位)
//    )
    //销售金额是否等于数量*单价   ?同一商品单价是否一样?
//    val unit3 = frame2.filter('销售金额.cast(DoubleType)==='商品单价.cast(DoubleType)*'销售数量.cast(DoubleType))
//    unit3.show()
//    unit3.groupBy("单位")
//      .agg(count("单位").as("num"))
////      .filter('单位.rlike("^\\d+$"))
//      .filter(!'单位.rlike("\\d+"))
//      .show()
//    println(unit3.count())  //32841
//    unit3.write.mode(SaveMode.Overwrite)
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("user","root")
//      .option("password","123")
//      .jdbc("jdbc:mysql://s1:3306/retail_db?useUnicode=true&amp;characterEncoding=UTF-8&amp;useSSL=false","qx",new Properties())
    //useUnicode=true&characterEncoding=UTF-8&useSSL=false
  }
}
