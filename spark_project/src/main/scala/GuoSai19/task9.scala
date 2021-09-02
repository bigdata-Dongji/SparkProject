package GuoSai19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object task9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("task9")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val frame = spark.read
      .option("header", true)
      //      .option("inferSchema", true)
      .csv("datas/hoteldata3.csv/part-r-00000-8b8d7b64-0242-4496-8c02-c6c7a49026cc.csv")
    //    frame.printSchema()
    frame.show()
    frame.createOrReplaceTempView("rawdata")

    //统计新疆、安徽、贵州、四川、海南各地四星/五星酒店的数量、平均评分、评论数、城市出租率、直销拒单率
    // 出租率 = 当月发生的总间夜数/当月所能提供的总房间数
    spark.sql(
      """
        |select `省份`,count(`酒店`),avg(`评分`),sum(`评论数`),
        |sum(`酒店总间夜`/`房间数`),sum(cast(regexp_replace(`酒店直销拒单率`,'%','') as double)/100.0)
        |from rawdata
        |where `省份` in ("新疆","安徽","贵州","四川","海南") and substr(`星级`,0,2) in ("四星","五星")
        |group by `省份`
        |""".stripMargin).show()

    frame.where('省份.isin("新疆","安徽","贵州","四川","海南") and substring('星级,0,2).isin("四星","五星"))
      .groupBy("省份").agg(
      count("酒店").as("四星/五星酒店的数量"),
      avg('评分.cast(DoubleType)).as("平均评分"),
      sum('评论数.cast(IntegerType)).as("总评论数"),
      sum('酒店总间夜.cast(DoubleType)/'房间数.cast(DoubleType)).as("城市出租率"),
      sum(regexp_replace('酒店直销拒单率,"%","").cast(DoubleType)/100.0).as("直销拒单率")
    ).show()
  }
}
