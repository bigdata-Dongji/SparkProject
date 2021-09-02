package Task

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object T_recruit {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("T_recruit")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read.json("E:\\数据文件\\recruitdata\\data.json")
    frame.show()
    /*
    若某个属性为空则删除这条数据；
    处理数据中的salary： 1)mK-nK：(m+n)/2； 2)其余即为0。
    按照MySQL表province 将城市编码转化为城市名；
    将结果存入MySQL表job中；
     */
    //去除空字符串用
//    frame.where("colname <> '' ").show()
    val frame1 = frame.withColumn("avg_salary",
      (split(regexp_replace('salary, "K", ""), "-")(0) +
        split(regexp_replace('salary, "K", ""), "-")(1)) / 2
    )
    val frame2 = frame1.na.replace("city_code", Map(
      "530" -> "北京"
    ))
    frame2.show()
    //rdd.filter(!_.trim.equals(""))
  }
}
