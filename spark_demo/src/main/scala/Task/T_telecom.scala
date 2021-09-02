package Task

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object T_telecom {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("T_telecom")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read.option("header",true).csv("E:\\数据文件\\telecomdata\\a.txt")
    frame.show()
    /*
      15733218050,15778423030,1542457633,1542457678,450000,530000
     呼叫者手机号	接受者手机号	开始时间戳（s）	接受时间戳（s）	呼叫者地址省份编码	接受者地址省份编码
     邓二,张倩,13666666666,15151889601,2018-03-29 10:58:12,2018-03-29 10:58:42,30,黑龙江省,上海市
     户名A	用户名B	用户A的手机号	用户B的手机号	开始时间	结束时间	通话时长	用户A地理位置	用户B地理位置
     */
    //  将时间戳转为日期格式，并求出通信时长
    val frame1 = frame.withColumn("startTime", from_unixtime(col("开始时间戳(s)")))
      .withColumn("stopTime", from_unixtime(col("接受时间戳(s)")))
    val frame2 = frame1.withColumn("useTime",col("接受时间戳(s)").cast(IntegerType)-col("开始时间戳(s)").cast(IntegerType))
    val frame3 = frame2.drop("开始时间戳(s)","接受时间戳(s)")
    frame3.show()
    //  读取数据库，手机号码与姓名映射表
    val frame4 = spark.read
        .option("header",true)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "511722")
      .jdbc("jdbc:mysql://localhost:3306", "mapreducedb.userphone", new Properties())
    frame4.show()
    //  根据手机号码查找姓名
    val dataFrame = frame4.select('phone.as("呼叫者手机号"),'trueName.as("call_name"))
    val dataFrame1 = frame4.select('phone.as("接受者手机号"),'trueName.as("accept_name"))
    val frame5 = frame3.join(dataFrame1,"接受者手机号").join(dataFrame,"呼叫者手机号")
    frame5.show()
    //  读取数据库，省份编码和省份映射表
    val frame6 = spark.read
      .option("header",true)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "511722")
      .jdbc("jdbc:mysql://localhost:3306", "mapreducedb.allregion", new Properties())
    frame6.show()
    //  根据省份编码查找省份
    val dataFrame2 = frame6.select('CodeNum.as("呼叫者地址省份编码"),'Address.as("call_address"))
    val dataFrame3 = frame6.select('CodeNum.as("接受者地址省份编码"),'Address.as("accept_address"))
    val frame7 = frame5.join(dataFrame3,"接受者地址省份编码").join(dataFrame2,"呼叫者地址省份编码")
    frame7.show()
    //  格式化字段
    frame7.drop("呼叫者地址省份编码")
      .drop("接受者地址省份编码")
      .withColumnRenamed("呼叫者手机号","call_phone")
      .withColumnRenamed("接受者手机号","accept_phone")
      .show()
  }
}
