package function

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/*
* RDD特点：
* 1.RDD是编程模型
* 2.RDD是数据集
* 3.RDD相互之间有依赖关系
* 4.RDD是可以分区的
* */
class wordcount {
  def main(args: Array[String]): Unit = {
    //创建sparkcontext
    val conf=new SparkConf().setMaster("local").setAppName("1")
    val sc=new SparkContext(conf)
    //加载文件
    val rdd1: RDD[String] =sc.textFile("E:\\数据文件\\Wordcount.txt")
    //数据处理
    val rdd2: RDD[String] =rdd1.flatMap(line => line.split(" "))
    val rdd3: RDD[(String, Int)] =rdd2.map(word => (word,1))
    val rdd4: RDD[(String, Int)] =rdd3.reduceByKey(_+_)
    //得出结果
    val result=rdd4.collect()

    result.foreach(item => println(item))
//    println(result)
  }

  @Test
  def sparkcontext(): Unit ={
    //sparkcontext的创建
    //创建sparkconf
    val conf=new SparkConf().setMaster("local").setAppName("2")
    //创建sparkcontext
    val sc=new SparkContext(conf)

    //关闭sparkcontext，释放资源
    sc.stop()
  }

  val conf=new SparkConf().setMaster("local").setAppName("1")
  val sc=new SparkContext(conf)

  //本地集合创建
  @Test
  def rddcreatlocal(): Unit ={
    val seq=Seq(1,2,3)
    val rdd1: RDD[Int] =sc.parallelize(seq = seq,2)
    val rdd2: RDD[Int] =sc.makeRDD(seq,2)
  }

  //从文件创建
  @Test
  def rddcreatfiles(): Unit ={
    val rdd1=sc.textFile("hdfs://....")
    //textFile传入的是路径
    //hdfs://   file://
    //是否分区
    //假如传入的是hdfs，分区是由hdfs文件的black决定
    //支持什么平台
    //支持dws和阿里云
  }

  //从RDD衍生
  @Test
  def rddcreatfromrdd(): Unit ={
    val rdd1=sc.parallelize(Seq(1,2,3))
    //在rdd中执行算子操作，生成新的rdd
    //原地计算
    //str.substring()返回新的，非原地计算
    //和字符串的方式很像，字符串可变吗？
    //rdd可变吗，不可变
    val rdd2=rdd1.map(item => item)

  }

  @Test
  def maptest(): Unit ={
    val rdd1=sc.parallelize(Seq(1,2,3))
    val rdd2=rdd1.map(item =>item*10)
    val rdd3=rdd2.collect()
    rdd3.foreach(line => println(line))
  }

  @Test
  def flatmaptest(): Unit ={
    val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","hello python"))
    val rdd2=rdd1.flatMap(item =>item.split(" "))
    val rdd3=rdd2.collect()
    rdd3.foreach(line => println(line))
  }
  //二元元祖代表KeyValue型数据


  @Test
  def reducebykeytest(): Unit ={
    val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","hello python"))
    val rdd2=rdd1.flatMap(item =>item.split(" "))
      .map(item => (item,1))
      .reduceByKey(_+_)
    val rdd3=rdd2.collect()
    rdd3.foreach(line => println(line))
  }


  /**
   * RDD的分区和shuffle过程
   *
   * 可以通过其他算子指定分区数：很多算子都可以指定分区数
   * 一般情况设计shuffle操作的算子都允许重新指定分区数
   * 一般这些算子，可以在最后一个参数的位置传入新的分区数
   * 如果没有重新指定分区数，默认从父RDD中继承分区数
   */

  /**
   *
   */
}
