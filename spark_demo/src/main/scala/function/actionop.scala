package function

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class actionop {
  val conf=new SparkConf().setMaster("local").setAppName("1")
  val sc=new SparkContext(conf)

  /**
   * reduceByKey和reduce有什么区别？
   * 1.reduce是一个action算子，reduceByKey是一个转换算子
   * 2.reduceByKey本质上是先按照key分组，然后聚合；reduce是针对一整个数据集进行聚合
   * 3.reduceByKey是针对kv类型数据进行计算；reduce可以针对所有类型的数据进行计算
   *
   * reduce算子是一个shuffle操作吗？
   * 1.Shuffle操作分为mapper和reducer，mapper将数据放入partition的函数计算
   * 求得分往哪个reducer，后分到对应的reducer中进行计算
   * 2.reduce操作并没有mapper和reducer，因为reduce算子会作用于RDD中的每一个分区
   * 然后在分区上求得局部结果，最终汇总到Driver中求得最终结果
   *
   * RDD中有五大属性，Partition在shuffle过程中使用
   * Partition只有KV类型的RDD才有
   */
  @Test
  def reduce(): Unit ={
    //函数中的curr参数，并不是value，而是一整条数据
    //reduce整体上的结果只有一个
    val rdd=sc.parallelize(Seq(("zs",19.0),("zs",15.0),("dj",16.0)))
    val result: (String, Double) =rdd.reduce((curr, agg)=>("zongjia",curr._2+agg._2))
    println(result)
  }

  //循环打印函数
  @Test
  def forech(): Unit ={
    val rdd=sc.parallelize(Seq(("zs",19.0),("zs",15.0),("dj",16.0)))
    rdd.foreach(println(_))
  }

  //收集结果函数
  @Test
  def collect(): Unit ={
    val rdd=sc.parallelize(Seq(("zs",19.0),("zs",15.0),("dj",16.0)))
    val rdd1: Array[Double] =rdd.map(item=>item._2-10).collect()
    println(rdd1(0))
  }

  /**
   * countByKey和count的结果相距很远，每次调用action都会生成一个job，job会运行获取结果
   * 所以在两个job中间有大量的log打出，其实就是在启动job
   *
   * countByKey的结果是Map（key，value->key的count）
   *
   * 数据倾斜，如果要解决数据倾斜的问题，是不是要先知道谁倾斜，通过countByKey可以查看key对应的数据总数
   * 从而解决数据倾斜问题
   */
  @Test
  def count(): Unit ={
    val rdd=sc.parallelize(Seq(("zs",19.0),("zs",15.0),("dj",16.0)))
    println(rdd.count())
    println(rdd.countByKey())
  }

  /**
   * take和takeSample都是获取数据，一个是直接获取，一个是采样获取
   * first：一般情况下action会从所有分区获取数据，相对来说速度比较慢，first只获取第一个元素
   * 所以first只会处理第一个分区，所以速度极快，无序处理所有数据
   */
  @Test
  def take(): Unit ={
    val rdd=sc.parallelize(Seq(1,2,3,4,5,6))
    rdd.take(3).foreach(println(_))
    println(rdd.first())
    //withReplacement是否放回
    rdd.takeSample(true,3).foreach(println(_))
  }

  /**
   * Action操作：
   *
   * collect：收集结果数据，一般用在转换操作后
   * reduce：汇总数据集
   * foreach：循环
   * countByKey，count：统计
   * take，takeSample，first：格式化取数据
   *
   */

  /**
   * RDD对数字类型的数据的额外支持Action
   *
   * count
   * mean
   * sum
   * max
   * min
   * variance 方差
   * samplevarian 从采样中计算方差
   * stdev  标准差
   * samplestdev
   */
  @Test
  def number(): Unit ={
    val rdd=sc.parallelize(Seq(1,2,3,4,5,6))
    println(rdd.max())
    println(rdd.min())
    println(rdd.mean())
    println(rdd.count())
    println(rdd.variance())
    println(rdd.stdev())
    println(rdd.sampleVariance())
    println(rdd.sampleStdev())
  }
}
