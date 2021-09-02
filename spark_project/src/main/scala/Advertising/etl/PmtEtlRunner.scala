package Advertising.etl

import java.util.Properties

import Advertising.config.ApplicationConfig
import Advertising.utils.{IpUtils, SparkUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
/**
 * 广告数据进行ETL处理，具体步骤如下：
 *    第一步、加载json数据
 *    第二步、解析IP地址为省份和城市
 *    第三步、数据保存至Hive表
 * TODO: 基于SparkSQL中DataFrame数据结构，使用DSL编程方式
 */
object PmtEtlRunner {
   // 3.   对数据进行ETL处理，调用ip2Region第三方库，解析IP地址为省份和城市
  def processData(dataframe: DataFrame): DataFrame = {
    // 获取SparkSession对象，并导入隐式转换
    val spark: SparkSession = dataframe.sparkSession
    // 解析IP地址数据字典文件分发
    spark.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    // TODO: 由于DataFrame弱类型（无泛型），不能直接使用mapPartitions或map，建议转换为RDD操作
    // a. 解析IP地址
    val newRowsRDD: RDD[Row] = dataframe.rdd.mapPartitions { iter =>
      // 创建DbSearcher对象，针对每个分区创建一个，并不是每条数据创建一个
      val dbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
      // 针对每个分区数据操作, 获取IP值，解析为省份和城市
      iter.map { row =>
        //  获取IP值
        val ipValue: String = row.getAs[String]("ip")
        //  调用工具类解析ip地址
        val region: Region = IpUtils.convertIpToRegion(ipValue, dbSearcher)
        //  将解析省份和城市追加到原来Row中，：+添加数据到最后，.+：添加数据在最前
        val newSeq: Seq[Any] = row.toSeq :+ region.province :+ region.city
        //  返回Row对象
        Row.fromSeq(newSeq)
      }
    }
    // b. 自定义Schema信息
    val newSchema: StructType = dataframe.schema // 获取原来DataFrame中Schema信息
      // 添加新字段Schema信息
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)

    // c. 将RDD转换为DataFrame
    val df: DataFrame = spark.createDataFrame(newRowsRDD, newSchema)
    // d. 添加一列日期字段，作为分区表的分区字段
    df.withColumn("date_str", date_sub(current_date(), 1).cast(StringType))
  }

   // 4.    保存数据至Parquet文件，列式存储
  def saveAsParquet(dataframe: DataFrame): Unit = {
    dataframe
      // 降低分区数目，保存文件时为一个文件
      .coalesce(1)
      .write
      // 选择覆盖保存模式，如果失败再次运行保存，不存在重复数据
      .mode(SaveMode.Overwrite)
      // 设置分区列名称
      .partitionBy("date_str")
      .csv("datas/pmt-etl2")
//      .parquet("datas/pmt-etl/")
  }

   // 4.   保存数据至Hive分区表中，按照日期字段分区 (练习写入mysql)
  def saveAsHiveTable(dataframe: DataFrame): Unit = {
    dataframe
      .write
//      .format("hive") // 一定要指定为hive数据源，否则报错
//      .mode(SaveMode.Append)
//      .partitionBy("date_str")
//      .saveAsTable("itcast_ads.pmt_ads_info")
      .mode(SaveMode.Overwrite)
      .option("driver",ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .jdbc("jdbc:mysql://localhost:3306","advertising.data",
        new Properties()
      )
  }

  def main(args: Array[String]): Unit = {
    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
//    System.setProperty("user.name", "root")
//    System.setProperty("HADOOP_USER_NAME", "root")

    //TODO 1. 创建SparkSession实例对象，导入隐士转换
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    //TODO 2. 从文件系统加载json数据
    val pmtDF: DataFrame = spark.read.json(ApplicationConfig.DATAS_PATH)
    //pmtDF.printSchema()
    //pmtDF.show(10, truncate = false)

    //TODO 3. 解析IP地址为省份和城市，并将省份列和城市列增加到dataframe中
    val etlDF: DataFrame = processData(pmtDF)
    //etlDF.printSchema()
//    etlDF.select($"ip", $"province", $"city", $"date_str").show(10, truncate = false)

    //TODO 4. 保存ETL数据至Hive分区表
//    saveAsParquet(etlDF)
    saveAsHiveTable(etlDF)

    //TODO 5. 应用结束，关闭资源
    //Thread.sleep(1000000)
    spark.stop()
  }

}
