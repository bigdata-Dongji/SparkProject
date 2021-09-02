package sql

import org.apache.spark.sql.SparkSession

object UDF {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("udf")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
   val source=Seq(
     ("Thin","cell phone",6000),
     ("Normal","tablet",1500),
     ("Mini","tablet",5500),
     ("Ultra thin","cell phone",5000),
     ("Very thin","cell phone",6000),
     ("Big","tablet",2500),
     ("Bendable","cell phone",3000),
     ("Foldable","cell phone",3000),
     ("Pro","tablet",4500),
     ("Pro2","tablet",6500)
   ).toDF("product","category","revenue")
    import org.apache.spark.sql.functions._
    //需求一:聚合每个类别的总价
    source.groupBy("category")
      .agg(sum("revenue")).show()
    //需求二:名称小写
    source.select(lower('product)).show()
    //需求三：价格变为字符串形式 6000    6K
//    source.select('revenue.toString).show()
    //自定义函数
    val functionstr = udf(tostr _)
    source.select(functionstr('revenue)).show()
  }
  def tostr(item:Long): String ={
    (item/1000)+"K"
  }
}
