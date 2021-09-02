package MTJournal

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/*
美团日志分析
①统计每个店铺分别有多少商品(SPU)
②统计每个店铺的总销售额
③统计每个店铺销售额最高的前三个商品输出内容包括店铺名，
 商品名和销售额其中销售额为0的商品不进行统计计算，
 例如：如果某个店铺销售为0则不进行统计
 */
/* 数据字段说明
字段名称	中文名称	数据类型
spu_id	商品spuID	String
shop_id	店铺ID	String
shop_name	店铺名称	String
category_name	类别名称	String
spu_name	SPU名称	String
spu_price	SPU商品售价	Double
spu_originprice	SPU商品原价	Double
month_sales	月销售量	Int
praise_num	点赞数	Int
spu_unit	SPU单位	String
spu_desc	SPU描述	String
spu_image	商品图片	String
 */
object meituan {
  def main(args: Array[String]): Unit = {
    //TODO 使用RDD查询
    val sc=new SparkContext(new SparkConf().setMaster("local").setAppName("meituan"))
    sc.setLogLevel("WARN")
//    sc.textFile("G:\\QQ\\2020省赛\\QX\\meituan_waimai_meishi.csv")
    val meituanRDD = sc.hadoopFile("G:\\QQ\\2020省赛\\QX\\meituan_waimai_meishi.csv",classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
      .map(pair => new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))
    //过滤首行，以及错误数据（行数字段<12）
    val filterRDD: RDD[Array[String]] = meituanRDD.filter(line => line.startsWith("spu_id") == false)
      .map(line => line.split(","))
      .filter(line => line.length == 12)
    //统计每个店铺分别有多少商品(SPU)
    println("每个店铺分别有多少商品(SPU)")
    filterRDD.map(line => (line(2),1))
      .reduceByKey(_+_).foreach(println)
    //统计每个店铺的总销售额
    println("每个店铺的总销售额")
    filterRDD.map(line => (line(2),line(5).toDouble.*(line(7).toInt)))
      .reduceByKey(_+_).foreach(println)
    // 安全的写法，防止有格式不对的值无法转型 double 或者 int 会报错：
    import scala.util._//先引入scala工具包：
//    filterRDD.map(line => (line(2),Try(line(5).toDouble).toOption.getOrElse(0.0).*(Try(line(7).toInt).toOption.getOrElse(0))))
//      .reduceByKey(_+_).foreach(println)
    // 统计每个店铺销售额最高的前三个商品输出内容包括店铺名，商品名和销售额
    // 其中销售额为0的商品不进行统计计算，例如：如果某个店铺销售为0则不进行统计
    println("每个店铺销售额最高的前三个商品")
    filterRDD.map(line => (line(2),line(4),line(5).toDouble.*(line(7).toInt)))
      .filter(line => line._3>0)
      .groupBy( line => line._1)//分组
      //根据分组后的结果中销量排序 .*(-1)为降序，取前三
      .flatMap(line => line._2.toList.sortBy(_._3.*(-1)).take(3))//flatMap会将其返回的数组全部拆散，然后合成到一个数组中
      .foreach(println)
    sc.stop()
    println("------------------------------------------------------------------------------------")
    println("------------------------------------------------------------------------------------")
    println("------------------------------------------------------------------------------------")
    //TODO 使用spark sql查询
    val spark = SparkSession.builder()
      .appName("jour")
      .master("local[6]")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //导入隐式转换
    import spark.implicits._
    //TODO 读取数据---*指定编码*
    val meituandf = spark.read
      .option("header",true)//将一行当schema信息
      .option("inferSchema",true)//自动判断类型
      .option("encoding","GBK")//改变编码
      .csv("G:\\QQ\\2020省赛\\QX\\meituan_waimai_meishi.csv")
    meituandf.createOrReplaceTempView("spu")
    //统计每个店铺分别有多少商品（SPU）
    spark.sql(
      """
        |select shop_name,count(*) as num
        |from spu
        |group by shop_name
        |""".stripMargin).show()
    //统计每个店铺的总销售额
    spark.sql(
      """
        |select shop_name,sum(spu_price * month_sales) as total
        |from spu
        |group by shop_name
        |""".stripMargin).show()
    //统计每个店铺销售额最高的前三个商品输出内容包括店铺名，商品名和销售额其中销售额为0的商品不进行统计,既月销售量为0的不统计
    spark.sql(
      """
        |select * from
        |(select shop_name,spu_name,spu_price * month_sales as total,
        |row_number() over (partition by shop_name order by spu_price * month_sales desc) number
        |from spu
        |where month_sales>0)t
        |where t.number<=3
        |""".stripMargin).show()
    //统计每个店铺的总销售额sales,店铺的商品总点赞数praise
    spark.sql(
      """
        |select concat(shop_id,shop_name),sum(spu_price*month_sales) total,sum(praise_num) praise
        |from spu
        |group by shop_id,shop_name
        |""".stripMargin).show()
    spark.stop()
  }
}
