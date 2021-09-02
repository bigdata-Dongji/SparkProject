package sparksql_hive

import org.apache.spark.sql.SparkSession
import org.junit.Test

class demo {

  @Test
  def text(): Unit ={
    val spark=SparkSession.builder()
      .master("local")
      .appName("biao")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    val createtablestr=
      """
        |create table student(
        |name string,
        |age int
        |)
        |""".stripMargin

    spark.sql("use mydb")
    spark.sql(createtablestr)
    spark.sql("insert into student values('dj',18)")

    //ctrl+d 运行复制框
  }
}
