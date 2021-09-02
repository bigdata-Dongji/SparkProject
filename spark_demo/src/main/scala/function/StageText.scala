package function

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class StageText {

  /**
   *
   * textfile 返回是HadoopRDD，继承了RDD
   * 里面有一个方法inputformat，就是MapReduce读取文件的方法
   */
  @Test
  def pmProcess(): Unit ={
    //创建sc对象
    val conf=new SparkConf().setMaster("local").setAppName("text")
    val sc=new SparkContext(conf)
    //读取文件
    val resource=sc.textFile("")
    //通过算子处理数据
    //    抽取数据，年，月，PM；返回结果：（（年，月），PM）
    //    清洗，过滤空的字符串和 NA
    //    聚合
    //    排序
    val result=resource.map( x => ((x.split(",")(1),x.split(",")(2)),x.split(",")(6)))
      .filter( x => StringUtils.isNotEmpty(x._2) && ! x._2.equalsIgnoreCase("NA"))
      .map( x => (x._1,x._2.toInt))
      .reduceByKey(_+_)
      .sortBy( x => x._2)
    //获取结果
    result.take(10).foreach(println(_))
    //关闭资源'
    sc.stop()
  }
}
