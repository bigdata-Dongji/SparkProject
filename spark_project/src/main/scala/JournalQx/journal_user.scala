package JournalQx

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object journal_user {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jour")
      .master("local[4]")
      //TODO 优化： 设置shuffle时分区数目（默认200，太多）
      .config("spark.sql.shuffle.partitions","4")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //读取数据
    val userdf = spark.read.option("header",true).csv("G:\\QQ\\2020省赛\\QX\\events\\users.csv")
    userdf.printSchema()

    //TODO 需求1：查看 user.csv 是否有重复数据
    //user_id是唯一的
    println("数据总行数："+userdf.count())
    println("user_id总行数"+userdf.select('user_id).distinct().count())

    //TODO 需求2：有多少用户没有输入或输入了无效的出生年份?
    /*
    使用birthyear转型为IntegerType的字段"f_birthyear"是因为birthyear可能存在脏数据，
    不是整形的数字，可能是字符串之类的，无法转为为整形，强行转化显示为null，
    所以此处增加一个字段 intbirthyear，筛选一次 intbirthyear isNull
    可以把 birthyear 所有空的或者无效脏数据找出来 转化为整形
    cast（）转换类型
     */
    val frame1 = userdf.select('user_id,'birthyear,'birthyear.cast(IntegerType).as("intbirthyear"))
    frame1.show()
    //取出年份为空的和转换失败的数据
    val frame2 = frame1.filter('intbirthyear.isNull)
    frame2.show()
    println("用户没有输入或输入了无效的出生年份"+frame2.count())
    //用户没有输入和输入了无效的出生年份 具体数据
    frame2.groupBy('birthyear).agg(count('birthyear)).show()

    //TODO 需求3：使用用户的平均出生年份来替换用户无效的、缺失的birthyear数据
    //将birthyear列转换为整形
    val frame3 = userdf.withColumn("birthyear",col("birthyear").cast(IntegerType))
    frame3.show()
    frame3.printSchema()
    //求出平均年份
    val avgdf = frame3.agg(avg("birthyear").cast(IntegerType).as("avg_year"))
    avgdf.show()
    //用平均年份替换 null和转换失败的数据
    //相当于 case when else
    val frame4 = frame3.join(avgdf).withColumn("birthyear",when('birthyear.isNull,'avg_year).otherwise('birthyear))
//    val frame4 = frame3.join(avgdf).withColumn("new_year",when('birthyear.isNull,'avg_year).otherwise('birthyear))
    frame4.show()
    //或者简单一点，在查询出结果的基础上将平均出生年份1988直接写死,将birthyear中 Null直接全部替换为1988
//    val frame5 = frame3.withColumn("birthyear",when(col("birthyear").isNull,lit(1988)).otherwise(col("birthyear")))

    //TODO 需求4：查询性别，发现"gender"字段中显示为 null,female,male，规范化，将null替换为unknown
    //查看性别列空值有多少
    frame4.groupBy('gender).agg(count('user_id)).show()
    //转换空值为"unknown"
    val frame5 = frame4.withColumn("gender",when('gender.isin("female","male"),value='gender).otherwise(lit("unknown")))
    //查看是否转换成功
    frame5.groupBy('gender).agg(count('user_id)).show()
    //删除join时新增的avg_year列
    val ClearUser = frame5.drop('avg_year)
    //得到完整的清洗数据
    ClearUser.show()//清洗结束
  }
}
