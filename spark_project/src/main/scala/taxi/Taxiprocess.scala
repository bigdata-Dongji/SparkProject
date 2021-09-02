package taxi

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
/*
需求：出租车在不同行政区的平均等待时间
 */
object Taxiprocess {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("Taxiprocess")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
//查看数据，判断使用dataset还是dataframe，偏查询的用dataframe
    // 这里最好使用dataset
    val taxidf = spark.read.option("header",true).csv("")

    //数据清洗与转换操作
//    val unit: Dataset[Trip] = taxidf.as[Trip]
//    taxidf.rdd.map(parse)
    val taxiparsed: RDD[Either[Trip, (Row, Exception)]] = taxidf.rdd.map(safe(parse))
    //可以通过如下方式来过滤出来所有异常的 ROW 和 Exception
//    taxiparsed.filter(_.isRight).map(e=>e.right.get._1)
//    taxiparsed.filter(_.isRight).map(e=>e.right.get._2)
    val taxiGood: Dataset[Trip] = taxiparsed.map(_.left.get).toDS()

    //编写UDF完成时长计算，将毫秒转换为小时单位
    val hours=(pickUpTime:Long,dropOffTime:Long)=>{
      val duration=dropOffTime-pickUpTime
      val hours=TimeUnit.HOURS.convert(duration,TimeUnit.MILLISECONDS)
      hours
    }
    val hoursudf=udf(hours)
    taxiGood.groupBy(hoursudf('pickUpTime,'dropOffTime) as "duration")
      .count()
      .sort("duration")
      .show()
    //结果显示就一条数据不正常，在一个小时以外，这条数据就没有什么意义，可以不用
    spark.udf.register("hours",hours)//注册sparkudf函数
    val taxiclean = taxiGood.where("hours(pickUpTime,dropOffTime) between 0 and 1 ")

  }

  /**
   * 作用就是封装 parse方法，捕获异常
   */
  def safe[P,R](f:P=>R):P=> Either[R,(P,Exception)]={
    new Function[P,Either[R,(P,Exception)]] with Serializable{
      override def apply(v1: P): Either[R, (P, Exception)] = {
        try{
          Left(f(v1))
        }catch {
          case e:Exception=>Right((v1,e))
        }
      }
    }
  }

  //RoW -> Trip
  //数据转换
  def parse(row : Row):Trip={
    val richRow=new RichRow(row)
    val license = richRow.getAs[String]("").orNull
    val pickUpTime=parseTime(richRow,"")
    val dropOffTime=parseTime(richRow,"")
    val pickUpX=parseLocation(richRow,"")
    val pickUpY=parseLocation(richRow,"")
    val dropOffX=parseLocation(richRow,"")
    val dropOffY=parseLocation(richRow,"")
    Trip(license,pickUpTime,dropOffTime,pickUpX,pickUpY,dropOffX,dropOffY)
  }

  def parseTime(row:RichRow,field:String):Long={
    // 转换时间类型的格式 SimpleDateFormat
    val formater=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.ENGLISH)
    //执行转换，获取Date对象，getTime获取时间戳
    val time: Option[String] =row.getAs[String](field)
    val maybeLong: Option[Long] = time.map(formater.parse(_).getTime)
    maybeLong.getOrElse(0L)
  }

  def parseLocation(row:RichRow,field:String):Double={
    //获取数据
    val location = row.getAs[String](field)
    //转换数据
    val locationoption: Option[Double] = location.map(_.toDouble)
    locationoption.getOrElse(0.0D)
  }
}

/*
dataframe 中的Row的包装类型，主要为了包装getAs方法
 */
class RichRow(row:Row){
  //Option返回结果可能为空，必须处理空值
  //Option对象本身提供了一些对于null的支持
  //为了返回Option提醒外面处理空值，提供处理方法
  def getAs[T](field:String):Option[T]={
    //1.判断 row.getAs 是否为空，row中对应的field是否为空
    if (row.isNullAt(row.fieldIndex(field))){
      // 为空-》返回None
      None
    }else{
      // 不为空-》返回Some
      Some(row.getAs[T](field))
    }
  }
}