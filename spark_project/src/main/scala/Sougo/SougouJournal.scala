package Sougo

import org.apache.spark.{SparkConf, SparkContext}
import com.hankcs.hanlp.HanLP
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
 * 1. 搜索关键词统计，使用HanLP中文分词
 * 2. 用户搜索次数统计
 * 3. 搜索时间段统计
 * 数据格式：
 * 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对
 * 应同一个用户ID
 */

object SougouJournal {
  def main(args: Array[String]): Unit = {
    // =================== 读取数据 ===================
    //构建SparkContext实例对象,读取本次SogouQ.reduced数据，封装到SougoRecord中 。
    val conf = new SparkConf().setAppName("sougou").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // TODO: 1. 本地读取SogouQ用户查询日志数据
//    val rawLogsRDD = sc.textFile("datas/sogou/SogouQ.reduced")
    //TODO textfile()不能设置编码，hadoopfile（）解决乱码，设置编码，比较老，命令死板，要记住
    val rawLogsRDD: RDD[String] =sc.hadoopFile("datas/sogou/SogouQ.reduced",classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
      .map(pair=>new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))

    // TODO: 2. 解析数据，封装到CaseClass样例类中
    val recordsRDD: RDD[SogouRecord] = rawLogsRDD
      // 过滤不合法数据，如空数据null，分割后长度不等于6
      .filter(log => null != log && log.trim.split("\\s+").length == 6)
      // 对每个分区中数据进行解析，封装到SogouRecord
      .mapPartitions{iter =>
        iter.map{log =>
          val arr: Array[String] = log.trim.split("\\s+")
          SogouRecord(
            arr(0), arr(1), arr(2).replaceAll("\\[|\\]", ""),
            arr(3).toInt, arr(4).toInt, arr(5)
          )
        }
      }
    println(s"Count = ${recordsRDD.count()}, First = ${recordsRDD.first()}")
    // 数据使用多次，进行缓存操作，使用count触发
    recordsRDD.persist(StorageLevel.MEMORY_AND_DISK).count()


    /*
    TODO: 3. 依据需求统计分析
    1. 搜索关键词统计，使用HanLP中文分词
    2. 用户搜索次数统计
    3. 搜索时间段统计
    */
    // ===================  搜索关键词统计 ===================
    // 1. 获取搜索词，进行中文分词
    val wordsRDD: RDD[String] = recordsRDD.mapPartitions{ iter =>
      iter.flatMap{record =>
        // 使用HanLP中文分词库进行分词
        val terms = HanLP.segment(record.queryWords.trim) //queryWords是用户查询词
        // 将Java中集合对转换为Scala中集合对象
        import scala.collection.JavaConverters._
        terms.asScala.map(term => term.word)
      }
    }
    println(s"Count = ${wordsRDD.count()}, Example = ${wordsRDD.take(5).mkString(",")}")

    // 2. 统计搜索词出现次数，获取次数最多Top10
    val top10SearchWords: Array[(String, Int)] = wordsRDD
      .map(word => (word, 1)) // 每个单词出现一次
      .reduceByKey((tmp, item) => tmp + item) // 分组统计次数
      .sortByKey(ascending = false) // 词频降序排序
      .take(10) // 获取前10个搜索词
    top10SearchWords.foreach(println)
//    tuple.swap转换第一位和第二位位置


    // ===================  用户搜索点击次数统计 ===================
    /*
    每个用户在搜索引擎输入关键词以后，统计点击网页数目，反应搜索引擎准确度
    先按照用户ID分组，再按照搜索词分组，统计出每个用户每个搜索词点击网页个数
    */
    val clickCountRDD: RDD[((String, String), Int)] = recordsRDD
      .map{record =>
        // 获取用户ID和搜索词
        val key = record.userId -> record.queryWords
        (key, 1)
      }
      // 按照用户ID和搜索词组合的Key分组聚合
      .reduceByKey((tmp, item) => tmp + item)
    clickCountRDD
      .sortBy(tuple => tuple._2, ascending = false)
      .take(10).foreach(println)

    println(s"Max Click Count = ${clickCountRDD.map(_._2).max()}")
    println(s"Min Click Count = ${clickCountRDD.map(_._2).min()}")
    println(s"Avg Click Count = ${clickCountRDD.map(_._2).mean()}")


    // ===================  搜索时间段统计 ===================
    /*
    从搜索时间字段获取小时，统计个小时搜索次数
    */
    val hourSearchRDD = recordsRDD
      // 提取小时
      .map{record =>
        // 03:12:50
        record.queryTime.substring(0, 2)
      }
      // 分组聚合
      .map(word => (word, 1)) // 每个单词出现一次
      .reduceByKey((tmp, item) => tmp + item) // 分组统计次数
      .sortBy(tuple => tuple._2, ascending = false)
    hourSearchRDD.foreach(println)

    // 释放缓存数据
    recordsRDD.unpersist()

    // 应用结束，关闭资源
    sc.stop()

  }
}
