package sparksql_hive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object hiveaccess {
  def main(args: Array[String]): Unit = {
    /**
     * 创建sparkseeion
     *    开启hive支持
     *    指定metastore的位置
     *    指定warehouse的位置
     */
    val spark=SparkSession.builder()
      .appName("hivexe")
      .enableHiveSupport()
      .config("hive.metastore.uris","thirft://s1:9083")
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sql(
      """
        |
        |""".stripMargin)

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
      .csv("")  //hdfs路径

    //写入数据
    val df1=df.where('age>10)
    df1.write.mode(SaveMode.Overwrite).saveAsTable("mydb.student")
    //打包程序运行
  }
}
