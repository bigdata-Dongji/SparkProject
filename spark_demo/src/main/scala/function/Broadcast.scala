package function

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Broadcast {

  val conf=new SparkConf()
//  val sc=new SparkContext(conf)

  val sc= SparkSession
    .builder()
    .master("local")
    .config(conf)
    .appName("a")
    .getOrCreate()


  /**
   * 资源占用大，又十个对应的value
   */
//  @Test
//  def bc1(): Unit ={
//    //假设数据很大
//    val v=Map("spark"->"http://spark.apche.org","scala"->"http://scala-lang.org")
//
//    //将其中的spark和scala转为对应的网址
//    val r=sc.parallelize(Seq("spark","scala"))
//
//    val result=r.map(v(_)).collect()
//    println(result)
//  }

  /**
   * 使用广播，大幅度减少value的复制
   */
  @Test
  def bc2(): Unit ={
    //假设数据很大
    val v=Map("spark"->"http://spark.apche.org","scala"->"http://scala-lang.org")

    //创建广播
    val bc=sc.sparkContext.broadcast(v)

    //将其中的spark和scala转为对应的网址
    val r=sc.sparkContext.parallelize(Seq("spark","scala"))

    //在算子中使用广播变量代替直接引用集合，只会复制和executor一样的数量
    //在使用广播前，复制map了task数量份
    //在使用广播后，复制次数和executor数量一致
    val result=r.map(bc.value(_)).collect()
    result.foreach(println(_))
  }
}
