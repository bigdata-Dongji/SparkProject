package sparksql_hive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

/**
 * mysql访问方式：本地运行，集群运行
 * 写入mysql数据时，使用本地运行。读取的时候使用集群运行
 */
object mysqlwrite {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local")
      .appName("mysqlxe")
      .getOrCreate()
    import spark.implicits._

    //读取数据
    val schema=StructType(
      Seq(
        StructField("name",StringType),
        StructField("age",IntegerType),
        StructField("score",FloatType)
      )
    )
    val df=spark.read
      .option("delimiter","\t")
      .schema(schema)
      .csv("") //本地路径

    val df1=df.where('age>10)
    df1.write
      .format("jdbc")
      .option("url","jdbc:mysql://s1:3306/database")
      .option("dbtable","student")
      .option("user","root")
      .option("password","000")
      .mode(SaveMode.Overwrite) //写入模式
      .save()

  }
}
