package Advertising.report

import Advertising.config.ApplicationConfig
import Advertising.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 针对广告点击数据，依据需求进行报表开发，具体说明如下：
           - 各地域分布统计：region_stat_analysis
           - 广告区域统计：ads_region_analysis
           - 广告APP统计：ads_app_analysis
           - 广告设备统计：ads_device_analysis
           - 广告网络类型统计：ads_network_analysis
           - 广告运营商统计：ads_isp_analysis
           - 广告渠道统计：ads_channel_analysis
 */
object PmtReportRunner {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkSession实例对象和导入隐士转换
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    // 2. 从Hive表中加载广告ETL数据，日期过滤   (测试从本地文件 / mysql表 )
//    val pmtDF: DataFrame = spark.read
//      .table("itcast_ads.pmt_ads_info")
//      .where($"date_str" === date_sub(current_date(), 1))
//    val etlDF = spark.read.csv("datas/pmt-etl1/part-r-00000-bebc7f20-7951-4c80-861f-eb278978637b.csv")
    val etlDF: DataFrame = spark.read.format("jdbc")
      .option("dbtable", "advertising.data")
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("driver",ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url", "jdbc:mysql://localhost:3306")
      .load()
//    etlDF.printSchema()
//    etlDF.show(10)
    //TODO 判断是否有数据，如果没有数据，结束程序，不需要再进行报表分析
//    if(etlDF.isEmpty{
//      System.exit(-1)
//    }
    // TODO： 由于多张报表的开发，使用相同的数据，所以缓存
//    etlDF.persist(StorageLevel.MEMORY_AND_DISK)

    // 3. 依据不同业务需求开发报表
    /*
    不同业务报表统计分析时，两步骤：
    i. 编写SQL或者DSL分析
    ii. 将分析结果保存MySQL数据库表中
    */

    // 3.1. 地域分布统计：region_stat_analysis
    RegionStateReport.doReport(etlDF)
    // 3.2. 广告区域统计：ads_region_analysis
    //AdsRegionAnalysisReport.doReport(etlDF)
    // 3.3. 广告APP统计：ads_app_analysis
    //AdsAppAnalysisReport.processData(etlDF)
    // 3.4. 广告设备统计：ads_device_analysis
    //AdsDeviceAnalysisReport.processData(etlDF)
    // 3.5. 广告网络类型统计：ads_network_analysis
    //AdsNetworkAnalysisReport.processData(etlDF)
    // 3.6. 广告运营商统计：ads_isp_analysis
    //AdsIspAnalysisReport.processData(etlDF)
    // 3.7. 广告渠道统计：ads_channel_analysis
    //AdsChannelAnalysisReport.processData(etlDF)

    //释放资源
//    etlDF.unpersist()
    // 4. 应用结束，关闭资源
    spark.stop()
  }
}
