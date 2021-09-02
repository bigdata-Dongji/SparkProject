package TyPe

import java.lang

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.junit.Test

class Typedtransformation {
  val spark=SparkSession.builder()
    .master("local")
    .appName("t1")
    .getOrCreate()
  import spark.implicits._

  @Test
  def trans1(): Unit ={
    val ds = Seq("hello spark","hello hadoop").toDS()
    ds.flatMap( _.split(" "))
  }

  /**
   * 将一个数据集转换成另一个数据集
   */
  @Test
  def trans2(): Unit ={
    val ds = spark.range(10)
    ds.transform(x=>x.withColumn("dj",'id)).show()
  }

  @Test
  def as(): Unit ={
    val schema=StructType(
      Seq(
        StructField("name",StringType),
        StructField("age",IntegerType),
        StructField("score",FloatType)
      )
    )

    val df: DataFrame = spark.read.schema(schema).option("delimiter","\t").csv("")
    //    val df: Dataset[Row] = spark.read.schema(schema).option("delimiter","\t").csv("")

    /**
     * 转换
     * 本质上：Dataset[Row].as[String] =》Dataset[String]
     */
    val unit: Dataset[String] = df.as[String] //根据数据转换
  }


  @Test
  def split(): Unit ={
    val ds: Dataset[lang.Long] = spark.range(15)
    //randomSplit,切多少份，权重多少
    val dataset: Array[Dataset[lang.Long]] =ds.randomSplit(Array(3,5,2))

    ds.sample(true,1)
  }

  //排序
  @Test
  def sort(): Unit ={
    val ds: Dataset[lang.Long] = spark.range(15)
    ds.orderBy('age.desc)
    ds.sort('age.asc)
  }

  //去重
  @Test
  def dropdupli(): Unit ={
    val ds: Dataset[lang.Long] = spark.range(15)
    ds.distinct()
    ds.dropDuplicates("age")  //指定列名，删除重复列
  }

  @Test
  def colle(): Unit ={
    val ds1: Dataset[lang.Long] = spark.range(15)
    val ds2=spark.range(5,15)

    //差集
    ds1.except(ds2)
    //并集
    ds1.union(ds2)
    //交集
    ds1.intersect(ds2)
    //limit
    ds1.limit(3 )
  }
}
