package JournalQx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
object journal_user_friends {
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
    val user_friendsdf = spark.read.option("header",true).csv("G:\\QQ\\2020省赛\\QX\\events\\user_friends.csv")
//  TODO  需求1：查询数据总量
    println("数据总行数："+user_friendsdf.count())
    user_friendsdf.show()

//  TODO  需求2：如何实现数据查询 每行数据中user_id,friends_id 一 一对应
    //行转列，使用 explode 算子，将 friends 字段中的friend_id 行转列，与user_id 一 一对应
    val user_frienddf=user_friendsdf.withColumnRenamed("user","user_id")
      .withColumn("friend_id",explode(split('friends," ")))
      .drop('friends)
    user_frienddf.show(10)

    // sql 语句实现行转列
    user_friendsdf.createOrReplaceTempView("user_friends")
    spark.sql(
      """
        |select user user_id,friend_id
        |from user_friends
        |lateral view explode(split(friends,' ')) as friend_id
        |""".stripMargin).show(10)

//  TODO  需求3：查询行转列之后的表中有多少有效数据 (friend_id非空)
    //多次使用，建议缓存
    user_frienddf.persist(StorageLevel.MEMORY_AND_DISK)
    //查看 friend_id 有没有空数据
//    user_frienddf.filter('friend_id.isNull).show()
    //查询 friend_id 有多少有效数据
//    println("有效数据:"+user_frienddf.filter('friend_id.isNotNull).distinct().count())
    //查询 df 总数据量
//    println("总数据量:"+user_frienddf.count())

    //有效数据比总数据少,存在重复数据
    //用 sql 语句查询重复数据
    user_frienddf.createOrReplaceTempView("user_friend")
    spark.sql(
      """
        |select user_id,friend_id,count(*) num
        |from user_friend
        |group by user_id,friend_id
        |having num>1
        |""".stripMargin).show()
    //使用 spark 查询重复数据
    user_frienddf.groupBy('user_id,'friend_id)
        .agg(count('user_id).as("num"))
        .filter('num>1)
        .show()

//  TODO  需求4：统计列表中谁拥有最多的朋友
    user_frienddf.distinct().groupBy('user_id)
        .agg(count('friend_id).as("friendnum"))
        .select('user_id,'friendnum)
        .orderBy('friendnum.desc).show()

    //不再使用，释放资源
    user_frienddf.unpersist()
  }
}
