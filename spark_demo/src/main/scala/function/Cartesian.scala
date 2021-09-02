package function

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Cartesian {
  /**
   * 窄依赖：一对一，没有shuffle，
   * 宽依赖：有shuffle
   */
  @Test
  def narrowDependenty(): Unit ={
    //需求:求得两个RDD之间的笛卡尔积
    val conf = new SparkConf().setMaster("local").setAppName("Cartesian")
    val sc = new SparkContext(conf)

    val rdd1=sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2=sc.parallelize(Seq("a","b","c"))

    //计算
    rdd1.cartesian(rdd2)
      .collect()
      .foreach(println(_))

  }
}
