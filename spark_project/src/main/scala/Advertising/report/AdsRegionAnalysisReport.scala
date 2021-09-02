package Advertising.report

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * 广告区域统计：ads_region_analysis，区域维度：省份和城市
 */
object AdsRegionAnalysisReport {
	
	/*
不同业务报表统计分析时，两步骤：
	i. 编写SQL分析
	ii. 将分析结果保存MySQL数据库表中
*/
	def doReport(dataframe: DataFrame): Unit = {
		val session = dataframe.sparkSession
		
		// ======================== 1. 计算基本指标 ========================
		// 注册为临时视图
		dataframe.createOrReplaceTempView("tmp_view_pmt")
		// 编写SQL并执行
		val reportDF: DataFrame = session.sql(
			ReportSQLConstant.reportAdsRegionKpiSQL("tmp_view_pmt")
		)
		
		// 保存至MySQL数据库表中
		saveReportToMySQL(reportDF)
	}
	
	/*
	不同业务报表统计分析时，两步骤：
		i. 编写SQL分析
		ii. 将分析结果保存MySQL数据库表中
	*/
	def doReportRate(dataframe: DataFrame): Unit = {
		val session = dataframe.sparkSession
		
		// ======================== 1. 计算基本指标 ========================
		// 注册为临时视图
		dataframe.createOrReplaceTempView("tmp_view_pmt")
		// 编写SQL并执行
		val reportDF: DataFrame = session.sql(
			ReportSQLConstant.reportAdsRegionSQL("tmp_view_pmt")
		)
		// ======================== 2. 计算三率 ========================
		reportDF.createOrReplaceTempView("tmp_view_report")
			val resultDF: DataFrame = session.sql(
			ReportSQLConstant.reportAdsRegionRateSQL("tmp_view_report")
		)
		resultDF.printSchema()
		resultDF.show(20, truncate = false)
		
		// 保存至MySQL数据库表中
		saveReportToMySQL(resultDF)
	}
	
	/**
	 * 将报表数据保存MySQL数据库，使用SparkSQL自带数据源API接口
	 */
	def saveReportToMySQL(reportDF: DataFrame): Unit = {
		reportDF.write
			// TODO: 采用Append时，如果多次运行报表执行，主键冲突，所以不可用
			.mode(SaveMode.Append)
			// TODO：采用Overwrite时，将会覆盖以前所有报表，更加不可取
			//.mode(SaveMode.Overwrite)
			.format("jdbc")
			.option("driver", "com.mysql.cj.jdbc.Driver")
			.option("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
			.option("user", "root")
			.option("password", "123456")
			.option("dbtable", "itcast_ads_report.ads_region_analysis")
			.save()
	}
	
}
