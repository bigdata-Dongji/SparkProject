package JournalQx

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object journal_event_attendees {
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
    val event_attendeesdf = spark.read.option("header",true).csv("G:\\QQ\\2020省赛\\QX\\events\\event_attendees.csv")
//    event_attendeesdf.printSchema()
//    event_attendeesdf.show(5)
//  TODO  需求1：表头为event,yes,maybe,invited, no, 将表头格式改为event_id,user_id,attend_type
    // 方法① 查询 event,yes 、 event,maybe 、 event,invited 、event,no 这四种字段组合，
    // 分别进行 行转列 ，最后在join组合一起即可(效率低且慢，浪费资源)
//    val yesdf = event_attendeesdf.select("event","yes")
//      .withColumnRenamed("event","event_id")
//      .withColumn("user_id",explode(split($"yes"," ")))
//      .withColumn("attend_type",lit("yes")).drop("yes")
//    val nodf = event_attendeesdf.select("event","no")
//      .withColumnRenamed("event","event_id")
//      .withColumn("user_id",explode(split($"no"," ")))
//      .withColumn("attend_type",lit("no")).drop("no")
//    val maybedf = event_attendeesdf.select("event","maybe")
//      .withColumnRenamed("event","event_id")
//      .withColumn("user_id",explode(split($"maybe"," ")))
//      .withColumn("attend_type",lit("maybe")).drop("maybe")
//    val inviteddf = event_attendeesdf.select("event","invited")
//      .withColumnRenamed("event","event_id")
//      .withColumn("user_id",explode(split($"invited"," ")))
//      .withColumn("attend_type",lit("invited")).drop("invited")
//    val resultdf: Dataset[Row] =yesdf.union(maybedf).union(inviteddf).union(nodf)
//    resultdf.show()
//    println(resultdf.distinct().count())
//    println(resultdf.count())
    //有两条重复数据
    //方法② 直接全部查询 yes,maybe,invited,no 四种受邀状态类型，放到1个集合Seq中，分类、聚合，一步到位(方法一的优化)
    val resultsdf = Seq("yes","maybe","invited","no").
      map(at => event_attendeesdf.select($"event".as("event_id"),col(at)).
        withColumn("user_id",explode(split(col(at)," "))).drop(col(at)).
        withColumn("attend_type",lit(at))).reduce((x,y) => x.union(y))
    resultsdf.show(10)
    println(resultsdf.count())
    println(resultsdf.distinct().count())
    //方法③ sql语句实现
    event_attendeesdf.createOrReplaceTempView("event_attendees")
    spark.sql(
      """
        |with final as
        |(
        |select distinct
        |event as event_id,
        |user_id,
        |'yes' as attend_type
        |from event_attendees
        |lateral view explode(split(yes," ")) t as user_id
        |union all
        |select distinct
        |event as event_id,
        |user_id,
        |'maybe' as attend_type
        |from event_attendees
        |lateral view explode(split(maybe," ")) t as user_id
        |union all
        |select distinct
        |event as event_id,
        |user_id,
        |'invited' as attend_type
        |from event_attendees
        |lateral view explode(split(invited," ")) t as user_id
        |union all
        |select distinct
        |event as event_id,
        |user_id,
        |'no' as attend_type
        |from event_attendees
        |lateral view explode(split(no," ")) t as user_id
        |)
        |select * from final
        |""".stripMargin).show()

  }
}
