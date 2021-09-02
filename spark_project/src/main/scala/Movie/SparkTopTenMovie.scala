package Movie


import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 需求：对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
 */
object SparkTopTenMovie {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("TopTenMovie")
      //TODO 优化： 设置shuffle时分区数目（默认200，太多）
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._
    //TODO 1. 读取电影评分数据，从本地文件系统读取
    val rawRatingsDS: Dataset[String] = spark.read.textFile("datas/movie/ratings.dat")
    //TODO 2. 转换数据,指定schema信息，封装到dataframe
    val ratingsDF: DataFrame = rawRatingsDS
      // 过滤数据
      .filter(line => null != line && line.trim.split("::").length == 4)
      // 提取转换数据
      .mapPartitions{iter =>
        iter.map{line =>
          // 按照分割符分割，拆箱到变量中
          val Array(userId, movieId, rating, timestamp) = line.trim.split("::")
          // 返回四元组
          (userId, movieId, rating.toDouble, timestamp.toLong)
        }
      } // 指定列名添加Schema
      .toDF("userId", "movieId", "rating", "timestamp")
    /*
    root
    |-- userId: string (nullable = true)
    |-- movieId: string (nullable = true)
    |-- rating: double (nullable = false)
    |-- timestamp: long (nullable = false)

    +------+-------+------+---------+
    |userId|movieId|rating|timestamp|
    +------+-------+------+---------+
    | 1| 1193| 5.0|978300760|
    +------+-------+------+---------+
    */
    //当某个数据集被多次使用，考虑缓存，因为底层是RDD
    ratingsDF.persist(StorageLevel.MEMORY_AND_DISK).count()

    // TODO 3.： 基于SQL方式分析
    // 第一步、注册DataFrame为临时视图
    ratingsDF.createOrReplaceTempView("view_temp_ratings")
    // 第二步、编写SQL
    /*
    1.按照电影进行分组，计算每个电影平均评分和评分人数
    2.过滤获取评分人数大于2000的电影
    3.按照电影评分降序排序，再按照评分人数降序排序
     */
    val top10MovieDF = spark.sql(
      """
        |SELECT
        | movieId, ROUND(AVG(rating), 2) AS avg_rating, COUNT(movieId) AS cnt_rating
        |FROM
        | view_temp_ratings
        |GROUP BY
        | movieId
        |HAVING
        | cnt_rating > 2000
        |ORDER BY
        | avg_rating DESC, cnt_rating DESC
        |LIMIT 10
        |""".stripMargin)
    //top10MovieDF.printSchema()
    top10MovieDF.show(10, truncate = false)

    println("===============================================================")

    // TODO 4. 基于 DSL=Domain Special Language（特定领域语言） 分析
    import org.apache.spark.sql.functions._//导入函数库

    val resultDF: Dataset[Row] = ratingsDF
      // 按照电影ID分组
      .groupBy($"movieId")
      //聚合：获取平均评分和评分次数
      .agg(
        round(avg($"rating"), 2).as("avg_rating"),
        count($"movieId").as("cnt_rating")
      ) // 过滤：评分次数大于2000
      .where($"cnt_rating" >= 2000)
      // 排序：先按照评分降序，再按照次数降序
      .orderBy($"avg_rating".desc, $"cnt_rating".desc)
      // 获取前10
      .limit(10)
    //resultDF.printSchema()
    resultDF.show(10)

    //缓存的数据集不载使用，释放缓存
    ratingsDF.unpersist()

    // TODO 5. 将分析的结果数据保存MySQL数据库和CSV文件
    // 结果DataFrame被使用多次，缓存
    resultDF.cache() // 由于结果数据很少，直接放内存即可

    // 1. 保存MySQL数据库表汇总
    resultDF
      .coalesce(1) // 数据少，考虑降低分区数目
      .write
      .mode(SaveMode.Overwrite)
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "511722")
      .jdbc("jdbc:mysql://localhost:3306/","advertising.toptenmovie",
        new Properties()
      )

    // 2. 保存CSV文件：每行数据中个字段之间使用逗号隔开
    resultDF
      .write.mode("overwrite")
      .option("header","true")
      .csv("datas/top10-movies/")

    // 释放缓存数据
    resultDF.unpersist()

    // 应用结束，关闭资源
//    Thread.sleep(10000000)
    spark.stop()
  }
}
