package YiQingDemo

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

/**
 * 疫情练习
 */
object epidemic_situation {
  def main(args: Array[String]): Unit = {
    //创建SparkSession，sparksql入口
    val spark = SparkSession.builder()
      .appName("text1")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //读取数据文件
    val civic_info = spark.read
      .option("header", "true")
      .option("nullValue", "\\N")//设置空值表达形式
      .option("inferSchema","true")//自动获取schema信息
      .csv("G:\\QQ\\2020省赛\\SparkSqlText\\civic_info.csv")

    //注册临时表
    civic_info.createTempView("civic")

    val ticket_info = spark.read
      .option("header", "true")
      .option("nullValue", "\\N")
      .option("inferSchema","true")
      .csv("G:\\QQ\\2020省赛\\SparkSqlText\\ticket_info.csv")

    ticket_info.createTempView("ticket")

    //直接用临时表进行sql查询
    println("湖北籍人员信息如下: ")
    spark.sql("select id_no, name from civic where province = '湖北'").show()

    println("来自武汉疫区人员如下: ")
    spark.sql("select id_no, name from civic where city = '武汉'").show()

    println("需要对员工进行隔离观察14天的公司: ")
    spark.sql("select distinct working_company from civic where province = '湖北'").show()

    println("有感染风险的车厢为: ")
    spark.sql("select distinct train_no,carriage_no from ticket where departure = '武汉'").show()

    println("需要执行隔离的人员: ")
    spark.sql("select passenger_name, passenger_id from ticket where carriage_no in (select distinct carriage_no from ticket where departure = '武汉')").show()

    // 真正操作大数据时不可能全打印出来, 可以count一下查看到的条数来做判断。
    spark.stop()  //关闭资源
  }
}
