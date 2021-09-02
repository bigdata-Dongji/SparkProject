package function

import org.apache.commons.lang.StringUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Cacheop {

  @Test
  def prepare(): Unit ={
    val conf=new SparkConf().setMaster("local").setAppName("text")
    val sc=new SparkContext(conf)
    val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","spark python","python spark"))
    val rdd2=rdd1.map(item =>(item.split(" ")(0),1))
    //filter过滤
    val rdd3=rdd2.filter(item =>StringUtils.isNotEmpty(item._1))
    val rdd4=rdd3.reduceByKey(_+_)
    //sortby默认升序,ascending = false降序
    val rdd5=rdd4.sortBy(item => item._2,ascending = true).first()
    val rdd6=rdd4.sortBy(item => item._2,ascending = false).first()
    sc.stop()
  }
    /**
     * 这里有两个first（），Action算子，也就是两个job
     * 那前面的代码是否会执行两遍？
     * 会，因为RDD是惰性求值的！！！
     *
     * transformation算子的作用：生成RDD，以及RDD之间的依赖关系
     * Action算子的作用：生成job，去执行job
     *
     * 这里每一个job都执行了两次shuffle（reduceByKey，sortBy）
     * 全局一共执行了四个shuffle，效率极低
     *
     * 如果能把处理完数据的RDD缓存下来，那会极大的提高效率，这就是缓存RDD的意义
     * 1.减少shuffle
     * 2.减少其他算子的执行
     * 3.减少执行速度，提高性能和效率
     * 4.方便容错
     */

    //RDD缓存的API：cache，persist

    @Test
    def cacheop(): Unit ={
      val conf=new SparkConf().setMaster("local").setAppName("text")
      val sc=new SparkContext(conf)

      //RDD的处理部分
      val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","spark python","python spark"))
      val rdd2=rdd1.map(item =>(item.split(" ")(0),1))
      val rdd3=rdd2.filter(item =>StringUtils.isNotEmpty(item._1))
      val rdd4=rdd3.reduceByKey(_+_).cache()//缓存RDD，就不用再执行上面的操作

      //两个RDD的Action操作
      //每个Action 都会完整的运行一下RDD的整个血统（前面代码）
      val rdd5=rdd4.sortBy(item => item._2,ascending = true).first()
      val rdd6=rdd4.sortBy(item => item._2,ascending = false).first()
      sc.stop()
    }


    @Test
    def presistop(): Unit ={
      val conf=new SparkConf().setMaster("local").setAppName("text")
      val sc=new SparkContext(conf)

      //RDD的处理部分
      val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","spark python","python spark"))
      val rdd2=rdd1.map(item =>(item.split(" ")(0),1))
      val rdd3=rdd2.filter(item =>StringUtils.isNotEmpty(item._1))
      val rdd4=rdd3.reduceByKey(_+_).persist(StorageLevel.MEMORY_ONLY)
      //指定存储级别：默认就是MEMORY_ONLY，内存

      //两个RDD的Action操作
      //每个Action 都会完整的运行一下RDD的整个血统（前面代码）
      val rdd5=rdd4.sortBy(item => item._2,ascending = true).first()
      val rdd6=rdd4.sortBy(item => item._2,ascending = false).first()
      sc.stop()
    }

  /**
   * 缓存级别是个技术活，有很多细节需要去思考：
   * 是否使用磁盘缓存？
   *      数据特别重要，经过很多次迭代计算，计算成本昂贵，不能丢失，即使丢失也要能恢复
   * 是否使用内存缓存？
   *      追求效率
   * 是否使用堆外内存？
   *      不安全，一般不用
   * 缓存前是否先序列化？
   *      取决于数据是否特别大，数据大的话，序列化好一点
   * 是否需要有副本？
   *      当workDriver挂掉时，RDD也没了，是否需要分发多个workDriver保存副本
   */

  /**
   * checkpoint和Cache的区别：
   * checkpoint可以保存数据到hdfs这类可靠的存储上，Cache和persist只能保存在本地的磁盘和内存中
   * checkpoint可以斩断RDD的依赖连，而persist和Cache不行
   * 因为checkpoint没有向上的依赖链，所以程序结束后依然存在，不会被删除，而Cache和persist会在程序结束后立刻被清除
   */

  @Test
  def checkpoint(): Unit ={
    val conf=new SparkConf().setMaster("local").setAppName("text")
    val sc=new SparkContext(conf)
    //设置保存checkpoint的目录
    sc.setCheckpointDir("hdfs路径或指定路径")


    //RDD的处理部分
    val rdd1=sc.parallelize(Seq("hello hadoop","hello spark","spark python","python spark"))
    val rdd2=rdd1.map(item =>(item.split(" ")(0),1))
    val rdd3=rdd2.filter(item =>StringUtils.isNotEmpty(item._1))
    var rdd4=rdd3.reduceByKey(_+_).persist(StorageLevel.MEMORY_ONLY)
    //指定存储级别：默认就是MEMORY_ONLY，内存

    //不准确的说，checkpoint是一个Action操作，也就是说
    //如果调用checkpoint，则会重新计算前面的代码，然后保存
    //所以，应该在checkpoint前，进行一次Cache
    rdd4 = rdd4.cache()
    rdd4.checkpoint()

    //两个RDD的Action操作
    //每个Action 都会完整的运行一下RDD的整个血统（前面代码）
    val rdd5=rdd4.sortBy(item => item._2,ascending = true).first()
    val rdd6=rdd4.sortBy(item => item._2,ascending = false).first()
    sc.stop()
  }
}
