package Task

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
/*
1、修改所有的时间格式为yyyy-MM-dd hh:mm:ss;
2、将字段loan_status_description的内容转化为中文；
COMPLETED：已完成
CHARGEOFF：冲销
CURRENT：进行中
DEFAULTED：违约
3、根据字段borrower_rate重新计算字段grade的值（所有区间均为左闭右开）：
A:0-0.05
B:0.05-0.1
C:0.1-0.15
D:0.15-0.2
E:0.2-0.35
F:0.25-0.3
G:0.3以上
4、保留每个等级中的每个起始日期的前三条数据（同一个起始日期按借款编号loan_number排序）。
 */
object T12_3 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local")
      .appName("text")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val result: DataFrame =spark.read.option("header",value = true).csv("G:\\QQ\\2020省赛\\综合测评2-1203\\Master_Loan_Summary.csv")
//    val date: Date = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm").parse("2013-12-01T00:00")
//    FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(date)
    val frame = result.withColumn("origination_date", date_format('origination_date, "yyyy-MM-dd HH:mm"))
      .withColumn("last_payment_date", date_format('last_payment_date, "yyyy-MM-dd HH:mm"))
      .withColumn("next_payment_due_date", date_format('next_payment_due_date, "yyyy-MM-dd HH:mm"))
//    frame.select('origination_date,'last_payment_date,'next_payment_due_date).show()
    val frame1 = frame.na.replace("loan_status_description",
      Map("COMPLETED" -> "已完成",
        "CHARGEOFF" -> "冲销",
        "CURRENT" -> "进行中",
        "DEFAULTED" -> "违约")
    )
//    A:0-0.05
//    B:0.05-0.1
//    C:0.1-0.15
//    D:0.15-0.2
//    E:0.2-0.35
//    F:0.25-0.3
//    G:0.3以上
    val frame2 = frame1.select('loan_number,
      when('borrower_rate.cast(DoubleType) >= 0 && 'borrower_rate.cast(DoubleType) < 0.05, "A")
        .when('borrower_rate.cast(DoubleType) >= 0.05 && 'borrower_rate.cast(DoubleType) < 0.1, "B")
        .when('borrower_rate.cast(DoubleType) >= 0.1 && 'borrower_rate.cast(DoubleType) < 0.15, "C")
        .when('borrower_rate.cast(DoubleType) >= 0.15 && 'borrower_rate.cast(DoubleType) < 0.2, "D")
        .when('borrower_rate.cast(DoubleType) >= 0.2 && 'borrower_rate.cast(DoubleType) < 0.25, "E")
        .when('borrower_rate.cast(DoubleType) >= 0.25 && 'borrower_rate.cast(DoubleType) < 0.3, "F")
        .when('borrower_rate.cast(DoubleType) >= 0.3, "G") as("grade")
    )
    val frame3 = frame1.drop("grade")
    val frame4 = frame3.join(frame2,"loan_number")
    frame4.show()
//    frame1.withColumn("grade",frame2.col("grade")).show() 不知道为什么不行，有问题
    //保留每个等级中的每个起始日期的前三条数据（同一个起始日期按借款编号loan_number排序）。
    frame1.createOrReplaceTempView("frame1")
    spark.sql(
      """
        |select * from(
        |select *,row_number() over (partition by grade order by origination_date,loan_number) number
        |        |from frame1)t where t.number<=3
        |""".stripMargin).show()
    frame1.select('*,row_number() over Window.partitionBy('grade).orderBy('origination_date,'loan_number) as "number")
      .where('number<=3).show()
  }
}
