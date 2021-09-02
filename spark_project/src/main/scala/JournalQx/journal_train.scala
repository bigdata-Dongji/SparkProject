package JournalQx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object journal_train {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jour")
      .master("local[6]")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //导入隐式转换
    import spark.implicits._
    //读取数据
    val traindf = spark.read.option("header",true).csv("G:\\QQ\\2020省赛\\QX\\events\\train.csv")
//    traindf.printSchema()
//    traindf.show(10)
//  TODO  需求1：查询有没有重复的数据
    println(traindf.count())
    println(traindf.select('user, 'event).distinct().count())
    // 存在重复的数据
//  TODO  需求2：如果存在，找出他们并且进行分析
    traindf.groupBy('user, 'event)
      .agg(count("user").as("cnt")).filter('cnt>1).show()
    /*
    dropDuplicates() 算子去重，可以指定具体字段，无论多少条重复数据，默认保留第一条
    distinct 去重，根据每一条数据，进行完整内容的比对和去重
     */
    //按照某列去重
    traindf.dropDuplicates("user","event").filter($"user" === lit("661151794") && $"event" === lit("187728438")).orderBy($"timestamp".asc).show(false)
    // 导入窗口函数：
    import org.apache.spark.sql.expressions.Window
    val df5 = traindf.withColumn("rn",row_number() over Window.partitionBy($"user",$"event").orderBy($"timestamp".desc))
  }
}
