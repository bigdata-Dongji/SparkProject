package function

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class accesslogagg {

  val conf=new SparkConf().setMaster("local").setAppName("1")
  val sc=new SparkContext(conf)

  @Test
  def flatmaptest(): Unit ={
    val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","spark python","python spark"))
    val rdd2=rdd1.map(item =>(item.split(" ")(0),1))
    //filter过滤
    val rdd3=rdd2.filter(item =>StringUtils.isNotEmpty(item._1))
    val rdd4=rdd3.reduceByKey(_+_)
    //sortby默认升序,ascending = false降序
    val rdd5=rdd4.sortBy(item => item._2,ascending = false)
    //take,打印前几条数据
    rdd5.take(2).foreach(x =>println(x))
//    val rdd3=rdd2.collect()
//    rdd3.foreach(line => println(line))
    sc.stop()
  }
}
