package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Test
//多维聚合：一个结果集中，包含总计，小计
class DWJuHe {
  val spark=SparkSession.builder()
    .appName("null")
    .master("local")
    .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
    .getOrCreate()
  import spark.implicits._


  @Test
  def mutilAgg(): Unit ={

    val schema=StructType(
      List(
        StructField("id",StringType),
        StructField("id",StringType),
        StructField("id",StringType),
        StructField("id",StringType),
        StructField("id",StringType)
      )
    )
    val df= spark.read
      .schema(schema)
      .option("header",true)
      .csv("")
    df.show()

    import org.apache.spark.sql.functions._
    //需求1：不同年，不用来源，PM值的均值
    val postandtime=df.groupBy('source,'year).agg(avg('pm) as "pm")
    //需求2：不同来源统计PM值的均值
    val post=df.groupBy('source).agg(avg('pm)as "pm")
      .select('source,lit(null) as "year",'pm)//创建year列与上个结果集合并
    //合并数据集
    postandtime.union(post)
      .sort('source,'year.desc,'pm)
      .show()

//    结果集就是：
//    cn 2011 78.0
//    cn 2012 78.0
//    cn 2013 78.0
//    ...
//    cn null 78.0  这一列代表全年的pm均值，有时候会出现这样的需求，这就叫多维聚合

  }

  //多维聚合函数API，有点复杂，不是很好理解
  @Test
  def rollup(): Unit ={
    import org.apache.spark.sql.functions._
    val sales=Seq(
      ("beijing",2016,100),
      ("beijing",2017,200),
      ("shanghai",2015,50),
      ("shanghai",2016,150),
      ("shenzhen",2017,50)
    ).toDF("city","year","amount")

    //rollup滚动分组，A，B两列：A(第一个分组列)，AB(第一和二分组列)，null(全部)
    sales.rollup('city,'year)
      .agg(sum('amount) as "amount")
      .sort('city.asc,'year.asc)
      .show()
    /*结果集：
+--------+----+------+
|    city|year|amount|
+--------+----+------+
|    null|null|   550|  这是这个产品所有城市的全部销售额
| beijing|null|   300|  这是北京的全部销售额
| beijing|2016|   100|  这是北京2016的全部销售额
| beijing|2017|   200|  ...
|shanghai|null|   200|
|shanghai|2015|    50|
|shanghai|2016|   150|
|shenzhen|null|    50|
|shenzhen|2017|    50|
+--------+----+------+
     */
  }


  @Test
  def cube(): Unit ={
    import org.apache.spark.sql.functions._
    val sales=Seq(
      ("beijing",2016,100),
      ("beijing",2017,200),
      ("shanghai",2015,50),
      ("shanghai",2016,150),
      ("shenzhen",2017,50)
    ).toDF("city","year","amount")

    //cube滚动分组，A，B两列：A(第一个分组列),B(第二个分组列)，AB(第一和二分组列)，null(全部)
    // cube滚动分组比rollup滚动分组多分组一列，有时候这一列可能也会需要
    sales.cube('city,'year)
      .agg(sum('amount) as "amount")
      .sort('city.asc,'year.asc)
      .show()
    /*结果集：
+--------+----+------+
|    city|year|amount|
+--------+----+------+
|    null|null|   550|  这是这个产品所有城市的全部销售额
|    null|2015|    50|  这是这一年的全部销售额
|    null|2016|   250|
|    null|2017|   250|
| beijing|null|   300|  这是北京的全部销售额
| beijing|2016|   100|  这是北京2016的全部销售额
| beijing|2017|   200|  ...
|shanghai|null|   200|
|shanghai|2015|    50|
|shanghai|2016|   150|
|shenzhen|null|    50|
|shenzhen|2017|    50|
+--------+----+------+
     */
  }

  //使用sql编写实现 cube 函数功能
  @Test
  def cubesql(): Unit = {
    val sales = Seq(
      ("beijing", 2016, 100),
      ("beijing", 2017, 200),
      ("shanghai", 2015, 50),
      ("shanghai", 2016, 150),
      ("shenzhen", 2017, 50)
    ).toDF("city", "year", "amount")

    sales.createOrReplaceTempView("pm_final")

    val result=spark.sql(
      """
        |select city,year,avg(amount) as amount
        |from pm_final
        |group by city,year
        |grouping sets ((city,year),(city),(year),())
        |order by city asc,year asc
        |""".stripMargin)

    result.show()
  }
}
