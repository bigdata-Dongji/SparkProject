package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Test

class ReadWrite {
  val spark = SparkSession.builder()
    .appName("sql")
    .master("local")
    .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
    .getOrCreate()
  import spark.implicits._  //导入隐式转换，RDD就可以转换成dataset,dataframe等

  @Test
  def read1(): Unit ={
    //读取文件两种方式
    spark.read
      .format("csv")  //文件类型
      .option("header",value = true)  //设置第一行为头信息
      .option("inferSchema",value = true) //设置schema结构化信息，自动推测列值类型
      .load("") //文件路径
      .show(10)

    spark.read
      .option("header",value = true)
      .option("inferSchema",value = true)
      .csv("")
      .show(10)


  }

  @Test
  def write1(): Unit ={
    val df = spark.read.option("header",value = true).csv("")
    //写入文件
    df.write.json("")
    df.write.format("json").save("")
    //写入模式
    //SaveMode.Overwrite  如果文件存在，覆盖写入文件
    //SaveMode.Append   如果文件存在，追加内容到文件
    //SaveMode.Ignore   如果文件存在，忽略，什么都不做
    //SaveMode.ErrorIfExists   如果文件存在，报错    默认是这种模式
    df.write.mode(SaveMode.ErrorIfExists).json("")
    //如果不指定写入文件格式，默认是 parquet 格式
    df.write.mode(SaveMode.ErrorIfExists).save("")

    //读取文件默认格式也是 parquet 格式，可以读取文件夹
    spark.read.load("").show()

    /**
     * 写入文件生成的是一个文件夹 和MapReduce的写入文件结果一样，为什么？
     * 因为spark的dataframe底层是RDD，RDD是可分区的，MapReduce也是可分区的
     */
  }

  /**
   * 表分区的概念不仅在 parquet 上有，其他格式的文件也可以指定表分区
   */
  @Test
  def parquetpartitions(): Unit ={
    val df = spark.read.option("header",value = true).csv("")

    //按照年和月分区保存文件
    //写出的文件夹结构
    // Fxxx
    //    year=xxxx
    //             month=xx
    //                      part-00000
    //             month=xx
    //    year=xxxx
    df.write
      .partitionBy("year","month")
      .save("")

    //读分区文件
    //出现问题:
    // 写分区表的时候，分区列不会包含在生成的文件中
    //直接读取子文件夹的话，分区信息会丢失
    //读取输出的文件夹，spark sql 会自动的发现分区
    spark.read.parquet("****/F***/year=***/month=***").printSchema()
    spark.read.parquet("****/F***").printSchema()
  }
//写入的json文件。并不是真正的json文件。而是json Line（jsonl）叫json行文件

  /**
   * toJSON的场景：
   * 处理完的数据，dataframe中如果是一个对象，如果其他系统或者消息队列只支持json数据
   * sparksql如果需要和这种系统进行整合的时候，就需要进行转换
   */
  @Test
  def json1(): Unit ={
    //读取文件两种方式
    val df=spark.read
      .format("csv")  //文件类型
      .option("header",value = true)  //设置第一行为头信息
      .option("inferSchema",value = true) //设置schema结构化信息，自动推测列值类型
      .load("") //文件路径

    df.toJSON //直接转json
  }


  @Test
  def json2(): Unit ={
    //读取文件两种方式
    val df=spark.read
      .format("csv")  //文件类型
      .option("header",value = true)  //设置第一行为头信息
      .option("inferSchema",value = true) //设置schema结构化信息，自动推测列值类型
      .load("") //文件路径

    val jsonrdd: RDD[String] = df.toJSON.rdd
    //直接读jsonRDD
    spark.read.json(jsonrdd).show()
  }
}
