package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object WindowFuntion {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("window")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
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
    //定义窗口
    val windowSpec = Window.partitionBy('category).orderBy('revenue.desc)
    //数据处理
    source.select('product,'category,row_number() over windowSpec as "rank")
      .where('rank<=2).show()

    //sql语句
    source.createOrReplaceTempView("sale")
    spark.sql(
      """
        |select * from (
        |select *,row_number() over (partition by category order by revenue desc) as rank
        |from sale
        |) where rank<=2
        |""".stripMargin)
    println("------------")
    source.select('*).show()
  }
}
