package TyPe

import java.lang

import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test

class Untypetransformation {
  val spark=SparkSession.builder()
    .master("local")
    .appName("t1")
    .getOrCreate()
  import spark.implicits._
  import org.apache.spark.sql.functions._

  @Test
  def select(): Unit ={
    //select可以选择需要的列
    val ds = spark.range(15)
    //在dataset中，select可以在任何位置调用
    val df1 = ds.select('age)
    //可以在查询中添加表达式
    ds.selectExpr("count(*)")
    //还可以导入函数包：
    import org.apache.spark.sql.functions._
    ds.select(expr("sum(age)"))
  }

  @Test
  def column(): Unit ={
    val ds: Dataset[lang.Long] = spark.range(10)
    //如果想使用函数功能
    //  使用funcation
    //  使用表达式，expr（"..."）
    ds.withColumn("random",expr("rand()"))  //新增列，列名random
    ds.withColumn("name_new",'name) //重命名列名，达不到效果，还需要删除旧列
    ds.withColumnRenamed("name","new_name") //改名
    ds.drop("name") //删除列
  }

  @Test
  def group(): Unit ={
    val ds = spark.range(10)
    //为什么groupbykey是有类型的,最主要的原因是因为groupbykey所生成的对象中的算子是有类型的
//    ds.groupByKey()
    //为什么groupby是无类型的,因为groupby所生成的对象中的算子是无类型的，针对列进行处理的
    ds.groupBy('age).agg(mean("age")) //按年龄求均值
  }
}
