package sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class Joinprocess {
  val spark=SparkSession.builder()
    .appName("join")
    .master("local")
    .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
    .getOrCreate()
  import spark.implicits._

  private val person = Seq((0,"lucy",1),(1,"lily",0),(2,"tim",2),(3,"danial",0)).toDF("id","name","cityid")
  person.createOrReplaceTempView("person")
  private val city = Seq((0,"beijing"),(1,"shanghai"),(2,"shengzhen")).toDF("id","name")
  city.createOrReplaceTempView("city")

  @Test
  def Join1(): Unit ={
    val df: DataFrame = person.join(city,person.col("cityid")===city.col("id"))
    df.show()
    df.createOrReplaceTempView("user_city")
    spark.sql("select * from user_city").show()
  }

//  @Test
//  def crossjoin(): Unit ={
//    spark.conf.set("spark.sql.crossJoin.enabled", "true")
//    person.crossJoin()
//  }

  @Test
  def inner(): Unit ={
    val df = person.join(city,
      person.col("cityid")===city.col("id"),
    "inner")  //默认就是内连接 inner
    df.show()
  }

  @Test
  def fullout(): Unit ={
    //内连接，就是只显示能连接上的数据，外连接包含一部分没有连接上的数据
    // 全外连接，指左右两边没有连接上的数据都显示出来
    person.join(city,
      person.col("cityid")===city.col("id"),
      "full")  //默认就是内连接 inner
      .show()
    spark.sql(
      """
        |select * from person full outer join city on person.cityid=city.id
        |""".stripMargin).show()
  }


  @Test
  def leftrightouter(): Unit ={
    //左外连接
    person.join(city,
      person.col("cityid")===city.col("id"),
      "left")
      .show()
    spark.sql(
      """
        |select * from person left join city on person.cityid=city.id
        |""".stripMargin).show()

    //右外连接
    person.join(city,
      person.col("cityid")===city.col("id"),
      "right")
      .show()
    spark.sql(
      """
        |select * from person right join city on person.cityid=city.id
        |""".stripMargin).show()
  }

  @Test
  def leftantisemi(): Unit ={
    //左连接anti，左边没有连接上的数据
    //左连接semi，左边连接上的数据
    person.join(city,
      person.col("cityid")===city.col("id"),
      "leftanti")//leftsemi
      .show()
    spark.sql(
      """
        |select * from person left anti join city on person.cityid=city.id
        |""".stripMargin).show()//left semi
    //另外查询字段也需查看一下，左边是否有
  }
}
