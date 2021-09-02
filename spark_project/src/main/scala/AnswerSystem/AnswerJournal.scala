package AnswerSystem

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType}
/*
日志数据示例	2019-06-16 14:24:34 INFO com.noriental.praxissvr.answer.util.PraxisSsdbUtil:45 [SimpleAsyncTaskExecutor-1] [020765925160]  req: set se0_34434412_8195023659593_8080r 1,resp: ok 14
日志结构	日志生成时间 INFO com.noriental.praxissvr.answer.util.PraxisSsdbUtil：45 [SimpleAsyncTasExecutor-1] [020765925160]req：set se难度系数_知识点ID_学生ID_题目IDr作答结果， resp：ok 14
字段	          解释	                                示例中的字段值
日志生成时间 	该条日志生成的时间	                        2019-06-1614：24：34
难度系数	  每道题的难度，分为4个等级易：1，中：2，难：3，极难：4	    3
知识点ID 	每个知识点有唯一的ID作为标识，一个知识点下可以有多道题目	  34434429
学生ID 	每个学生有唯一的ID作为标识	                        8195023659597
题目ID 	每个题目有唯一的ID作为标识，一个题目只属于一个知识点	      8467
作答结果	    错误：0，半对：0.5，全对：1                          0
 */
object AnswerJournal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jour")
      .master("local[6]")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //导入隐式转换
    import spark.implicits._
    val logRDD = spark.sparkContext.textFile("G:\\QQ\\2020省赛\\QX\\answer_question.log")
    println(logRDD.first())
    //提取字段 知识点ID，学生ID，题目ID，作答结果
    val etlRDD: RDD[(String, String, String, String)] = logRDD.map(line => line.split(" "))
      .map(line => (line(9).split("_"), line(10).split(",")))
      //去除 题目ID 字段后缀中的 r
      .map(line => (line._1(1), line._1(2), line._1(3).substring(0, line._1(3).length - 1), line._2(0)))
    println(etlRDD.first())
    // 将RDD转为dataframe
    val answerdf = etlRDD.toDF("knowledge_id","student_id","subject_id","result")
    //    answerdf.show()
    //    answerdf.printSchema()
    //统计每个学员总分、答题的试题数和正确率,  正确率=总分/答题的试题数
    answerdf.groupBy('student_id)
      .agg(sum('result.cast(DoubleType)).as("total"),count('subject_id).as("num"))
      .select('student_id,'total,'num,'total/'num)
      .show()
    //统计每个做对，做错，半对的题目列表,题目id 以逗号分割
    /* sql语句：
    select student_id,
    concat_ws('',collect_list(t.right)) right,
    concat_ws('',collect_list(t.half)) half,
    concat_ws('',collect_list(t.error)) error
    from (select student_id,
    case when score=1.0 then concat_ws(",",collect_list(question_id)) else null end right,
    case when score=0.5 then concat_ws(",",collect_list(question_id)) else null end half,
    case when score=0.0 then concat_ws(",",collect_list(question_id)) else null end error
    from answertable group by student_id,score) t
    group by student_id;
    */
//    answerdf.select('knowledge_id, when(concat_ws(",",collect_list('subject_id)),col("result").cast(DoubleType)===1.0).otherwise(null),
//      when(concat_ws(",",collect_list('subject_id)),col("result").cast(DoubleType)===0.5).otherwise(null),
//      when(concat_ws(",",collect_list('subject_id)),col("result").cast(DoubleType)===0.0).otherwise(null)
//    ).show()
  }
}
