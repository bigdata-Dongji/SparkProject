package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.junit.Test

class Nullprocessor {

  val spark=SparkSession.builder()
    .appName("null")
    .master("local")
    .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
    .getOrCreate()
  import spark.implicits._

  //空类型的空值处理
  @Test
  def nullandnan(): Unit ={
    /*
    读取数据：
            1.通过spark.csv自动推断类型来读取，推断数字的时候会将nan推断为字符串
            option（"inferSchema",true）
            2.直接读取字符串，在后续的操作中使用map算子转类型
            spark.read.csv().map(item=>....)
            3.指定schema
     */
    val schema=StructType(
      List(
        StructField("id",IntegerType),
        StructField("id",LongType),
        StructField("id",DoubleType)
      )
    )
//    Double.NaN
    val df=spark.read
      .option("header",true)
      .schema(schema)
      .csv("")

    df.show()

    //  缺失值处理：丢弃
//    2019  10  12  nan
//    规则：
//        1.any，只要有一个nan就丢弃
    df.na.drop("any").show()
    df.na.drop().show()
//        2.all，所有列都是nan的行才丢弃
    df.na.drop("all").show()
//        3.某些列的规则
    df.na.drop("any",List("year","month","day")).show()

    //  缺失值处理：填充
//    规则：
//        1.针对所有列数据进行默认值填充
    df.na.fill(0).show()
//        2.针对特定列填充
    df.na.fill(0,List("year","month","day")).show()
  }

  //字符串类型的空值处理
  @Test
  def strprocessor(): Unit ={
    val df=spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("")
    df.show()
    //  缺失值处理：丢弃
    df.where('PM=!="NA").show
    ()
    //  缺失值处理：替换
    //select id,age,case
    // when... then...
    // when... then...
    // else...
    import org.apache.spark.sql.functions._
    //将字符串空值替换空类型空值，非字符串空值替换和空类型一样的类型，保证这一列的数据类型一致
    df.select(
      'id,'year,'month,'day,'hour,
      when('PM_dongsi==="NA",Double.NaN)//替换Double类型的空值
        .otherwise('PM_dongsi cast DoubleType)//替换Double类型
        .as("pm")//取别名
    ).show()

//  原类型和转换过后的类型，必须一致
    df.na.replace("PM_dongsi",Map("NA" -> "NAN")).show()
  }
}
