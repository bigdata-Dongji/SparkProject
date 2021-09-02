package function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.Test

class transformationop {

  val conf=new SparkConf().setMaster("local").setAppName("1")
  val sc=new SparkContext(conf)

  /*
  * mappartitions和map算子是一样的，只是map是针对每一条数据进行转换，
  * mappartitions针对每一分区的数据进行转换，所以：
  * 1.map的func参数是单条数据，mappartitions的func参数是一个集合（一个分区的所有数据）
  * 2.map的func返回值是单条数据，mappartitions的func返回值是一个集合
  * */

  @Test
  def mappartitions(): Unit ={
    val seq=Seq(1,2,3,4,5,6)
    sc.parallelize(seq,2)
      .mapPartitions(item =>{
        item.foreach(item=>println(item))
        item
      })
      .collect()
  }

  @Test
  def mappartitions2(): Unit ={
    val seq=Seq(1,2,3,4,5,6)
    sc.parallelize(seq,2)
      .mapPartitions(item =>{
        //遍历item中的数据进行转换，转换完成以后
        //item是scala中的数据类型
        item.map(item => item*10)
      })
      .collect()
      .foreach(item => println(item))
  }


  /**
   * mappartitions和mappartitionswithindex的区别是func中多一个参数，是分区号
   */
  @Test
  def mappartitionswithindex(): Unit ={
    val seq=Seq(1,2,3,4,5,6)
    sc.parallelize(seq,2)
      .mapPartitionsWithIndex((index,item)=>{
        item.foreach(item=>println(item+"-"+index))
        item
      })
      .collect()
  }


  /**
   * filter 可以过滤数据集中的一部分元素
   * filter 接收的函数，参数是每一个元素，如果函数返回true，当前元素就会被加入新数据集
   * 如果返回false，当前元素会被过滤
   */
  @Test
  def fiflter(): Unit ={
    val seq=Seq(1,2,3,4,5,6)
    sc.parallelize(seq,2)
      .filter(item => item%2==0)
      .collect()
      .foreach(item => println(item))
  }

  /**
   * sample作用：把大数据集变小，尽可能的减少数据集的损失
   * withReplacement，指定True的情况下，数据集可能重复，false，不重复
   */
  @Test
  def sample(): Unit ={
    val seq=Seq(1,2,3,4,5,6)
    sc.parallelize(seq,2)
      .sample(false,0.3)
      .collect()
      .foreach(item => println(item))
  }

  /**
   * mapvalues也是map，只不过map作用于整条数据，mapvalues只作用于value
   */
  @Test
  def mapvalues(): Unit ={
    val seq=Seq(("map",1),("map",2),("map",3),("map",4),("map",5),("map",6))
    sc.parallelize(seq,2)
      .mapValues(item=>item*10)
      .collect()
      .foreach(item => println(item))
  }


  /**
   * 交集
   */
  @Test
  def intersection(): Unit ={
    val rdd1=sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2=sc.parallelize(Seq(4,5,6,7,8,9))
    rdd1.intersection(rdd2)
      .collect()
      .foreach(item => println(item))
  }

  /**
   * 并集,可以有重复的
   */
  @Test
  def union(): Unit ={
    val rdd1=sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2=sc.parallelize(Seq(4,5,6,7,8,9))
    rdd1.union(rdd2)
      .collect()
      .foreach(item => println(item))
  }

  /**
   * 差集
   */
  @Test
  def subtract(): Unit ={
    val rdd1=sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2=sc.parallelize(Seq(4,5,6,7,8,9))
    rdd1.subtract(rdd2)
      .collect()
      .foreach(item => println(item))
  }

//11.30
  /**
   * groupbykey运算结果的格式：（key，（value1，value2...））
   * reducebykey能不能在map端做combiner，能不能减少IO
   * groubykey在map端做combiner有没有意义？ 没有的...
   */
  @Test
  def groupbykey(): Unit ={
    val seq=Seq(("map",1),("map",2),("reduce",3),("map",4),("reduce",5),("map",6))
    sc.parallelize(seq)
      .groupByKey()
      .collect()
      .foreach(item => println(item))
  }

  /**
   *combineByKey接收三个参数
   *转换数据的函数，初始函数，作用于每一条数据，用于开启整个计算
   * 在分区上进行聚合，把所有分区的结果聚合为最终结果
   */
  @Test
  def combiner(): Unit ={
    val seq=Seq(("map",50),("map",40),("reduce",60),("map",80),("reduce",90),("map",20))
    //算子操作，combinebykey求平均值
    //  1.createcombiner转换数据
    //  2.mergevalue分区上的聚合
    //  3.mergecombiners把所有分区的结果再次聚合
    val rdd1: RDD[(String, Int)] =sc.parallelize(seq)
    val rdd2=rdd1.combineByKey(
      curr=>(curr,1),
      (c:(Int,Int),v) =>(c._1+v,c._2+1),
      (c:(Int,Int),v:(Int,Int))=>(c._1+v._1,c._2+v._2)
    )
//    rdd2.collect().foreach(item=> println(item))
    //结果：(map,(190,4))
    //      (reduce,(150,2))
    val rdd3=rdd2.map(item=>(item._1,item._2._1/item._2._2))
    rdd3.collect().foreach(item=> println(item))

  }

  /**
   * foldByKey和reducebykey的区别是可以指定初始值
   * foldByKey和scala中的foldleft和foldright区别是，初始值作用于每一个数据
   * foldleft和foldright作用于整体
   */
  @Test
  def fold(): Unit ={
    val seq=Seq(("map",1),("map",2),("reduce",3),("map",4),("reduce",5),("map",6))
    sc.parallelize(seq)
      .foldByKey(10)((curr,agg)=>curr+agg)
      .collect()
      .foreach(item => println(item))
  }

  /**
   * aggregateByKey
   * zerovalue 制定初始值
   * seqop作用于每一个元素，根据初始值进行计算
   * combop将seqop处理过的结果聚合
   * ？
   */
  @Test
  def aggregate(): Unit ={
    //1.将所有商品的价格打八折
    //2.聚合所有商品的打折价格
    val rdd=sc.parallelize(Seq(("zs",10.0),("zs",15.0),("dj",20.0)))
      .aggregateByKey(0.8)((c,v) => v*c, (curr,agg)=>curr+agg)
    rdd.collect().foreach(println(_))
  }

  @Test
  def join(): Unit ={
    val rdd1=sc.parallelize(Seq(("a",1),("a",2),("b",1)))
    val rdd2=sc.parallelize(Seq(("a",11),("a",1),("a",12)))
    rdd1.join(rdd2).collect().foreach(println(_))
  }


  //12.1
  /**
   * sortBy可以作用于任何类型数据的RDD，sortByKey只有kv类型数据的RDD才有
   * sortBy可以按照任何部分来排序，sortByKey只能按照key排序
   * sortByKey写法简单
   */
  @Test
  def sort(): Unit ={
    val rdd1=sc.parallelize(Seq(5,7,9,3,8,6,2,17,1))
    val rdd2=sc.parallelize(Seq(("zs",19.0),("zs",15.0),("dj",16.0)))
    rdd1.sortBy(item=>item)
    rdd2.sortBy(item=>item._2)
    rdd2.sortByKey().collect().foreach(println(_))
  }

  /**
   * repartition进行分区的时候，默认是shuffle的
   * coalesce进行分区的时候，默认是不shuffle的，coalesce默认不能增大分区
   */
  @Test
  def partion(): Unit ={
    val rdd1=sc.parallelize(Seq(5,7,9,3,8,6,2,17,1),2)
    //修改分区数
    println(rdd1.repartition(1).partitions.size)
    //减少分区，默认shuffle为false，不能增大分区，改为True才可以
    println(rdd1.coalesce(1).partitions.size)
    println(rdd1.coalesce(5,shuffle = true).partitions.size)
  }


  /**
   * 转换操作（transformation）算子：
   * （所有转换算子都是惰性的，在执行的时候并不会真的去调度执行
   * 求得结果，而是只生成了对应的RDD；只有在Action操作的时候
   * 才会真的运行求得结果）
   *
   * 转换：    map，mapPartitions，mapValues
   * 过滤：    filter，sample
   * 集合操作：Intersection，union，subtract
   * 聚合操作：reduceByKey，groupByKey，combineByKey，foldByKey，aggregateByKey
   *          sortBy，sortByKey
   * 重分区：  repartition，coalesce
   */

}
