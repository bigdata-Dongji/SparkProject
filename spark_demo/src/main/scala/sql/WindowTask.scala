package sql

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.Window

object WindowTask {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("windowstask")
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
  //需求：求每类商品和此类商品最贵的差价

    //定义窗口，按照分类进行倒序
    val windowSpec = Window.partitionBy('category).orderBy('revenue.desc)
    //找到最贵商品价格
    val maxprice: Column =max('revenue) over windowSpec
    //结果
    source.select('product,'category,'revenue,(maxprice-'revenue) as "cha")
      .show()
  }
}
