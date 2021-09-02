package sql



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.junit.Test
/**
 *sparksql:命令式和声明式（sql）API编程
 *
 * 命令式的优点：针对过程
 * 操作粒度更细，能够控制数据的每一个处理环节
 * 操作更明确，步骤更清晰，容易维护
 * 支持非结构化数据的操作
 *
 * 声明式（sql）的优点：针对结果
 * 表达清晰
 *
 *spark处理数据类型
 * sparkRDD主要用于处理半结构化和非结构化数据
 * sparksql主要用于处理结构化数据
 */

class Stage_one {

//  @Test
  ////  def rddtext(): Unit ={
  ////    val conf = new SparkConf().setMaster("local").setAppName("rdd")
  ////    val sc = new SparkContext(conf)
  ////
  ////    sc.parallelize(Seq("rdd","rdd","sql","sql"))
  ////      .map((_,1))
  ////      .reduceByKey(_+_)
  ////      .collect()
  ////      .foreach(println(_))
  ////    sc.stop()
  ////  }

  @Test
  def dstext(): Unit ={
    //java.net.URISyntaxException: Relative path in absolute URI: file:G:/ideaproject/spark_demo/spark-warehouse
    //spark调试创建本地文件没有
    //SparkSession是spark推出的一个spark新入口，常用于sparksql处理结构化数据和数据库的入口
    //SparkSession支持老的SparkContext，支持更多数据源，有一套完整的读写体系
    val spark = new SparkSession.Builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()

    import spark.implicits._  //引式转换，后面的RDD就可以转换成DATAset等

    val sqlrdd=spark.sparkContext.parallelize(Seq(Person("zs",18),Person("ls",21)))

    val ds=sqlrdd.toDS()
    //查询age大于10小于20的
    val result=ds.where('age>10)
      .where('age<20)
      .select('name)
      .as[String]
    //结果展示
    result.show()


    spark.stop()
  }


  @Test
  def dftext(): Unit ={
    val spark = new SparkSession.Builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()

    import spark.implicits._  //引式转换，RDD就可以转换成dataset,dataframe等

    val sqlrdd=spark.sparkContext.parallelize(Seq(Person("zs",18),Person("ls",21)))

    val df=sqlrdd.toDF()
    df.createOrReplaceTempView("person")

    val dataFrame = spark.sql("select * from person where age>10 and age<20")
    dataFrame.show()
  }


  @Test
  def dataset1(): Unit ={
    val spark = new SparkSession.Builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()

    import spark.implicits._  //导入隐式转换，RDD就可以转换成dataset,dataframe等

    val sqlrdd=spark.sparkContext.parallelize(Seq(Person("zs",18),Person("ls",21)))

    val dataset=sqlrdd.toDS()

    //dataset支持强类型的API
    dataset.filter(x=>x.age>10).show()
    //dataset支持弱类型的API
    dataset.filter('age>10).show()
    dataset.filter($"age">10).show()
    //dataset可以直接编写sql表达式
    dataset.filter("age>10").show()
    //无论dataset中放什么类型的数据，最终执行计划的RDD都是InternalRow
    val excuRDD: RDD[InternalRow] =dataset.queryExecution.toRdd
//    dataset.explain()
  }

  /**
   * dataset查看执行计划API
   * queryExecution 逻辑执行计划
   * explain  物理执行计划
   */


  @Test
  def dataset2(): Unit ={
    val spark = new SparkSession.Builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()

    import spark.implicits._  //导入隐式转换，RDD就可以转换成dataset,dataframe等

//    val sqlrdd=spark.sparkContext.parallelize(Seq(Person("zs",18),Person("ls",21)))
//    val dataset=sqlrdd.toDS()
    val dataset: Dataset[Person] =spark.createDataset(Seq(Person("zs",18),Person("ls",21)))
    //通过dataset底层的RDD[InternalRow]，通过decoder转成了和dataset一样类型的RDD
    val typerdd: RDD[Person] = dataset.rdd
    //直接获取到已分析和解析过的dataset的执行计划，从中拿到RDD
    val excuRDD: RDD[InternalRow] =dataset.queryExecution.toRdd
  }



  @Test
  def dataframe1(): Unit ={
    val spark = SparkSession.builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()
    import spark.implicits._  //导入隐式转换，RDD就可以转换成dataset,dataframe等
    val frame: DataFrame = Seq(Person("zs",18),Person("ls",21)).toDF()
    frame.where('age>10)
      .select('name)
      .show()
  }

  /**
   * 一般处理数据都差不多是ETL这个步骤
   * E  ->抽取
   * T  ->处理，转换
   * L  ->装载，落地
   *
   * spark 代码编写的套路
   * 1.创建 dataframe，dataset，rdd ，制造或者读取数据
   * 2.通过 dataframe，dataset，rdd 的API来进行数据处理
   * 3.通过 dataframe，dataset，rdd 进行数据落地
   */


  @Test
  def dataframe2(): Unit ={
    val spark = SparkSession.builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()
    import spark.implicits._  //导入隐式转换，RDD就可以转换成dataset,dataframe等
    val list: Seq[Person] = Seq(Person("zs",18),Person("ls",21))
    //dataframe创建方式

    //  1.toDF（）
    val df1 = list.toDF()
    //  2.createDataFrame（）
    val df2 = spark.createDataFrame(list)
    //  3.read（）
    val df3 = spark.read.csv("")

  }


  @Test
  def dataframe3(): Unit ={
    val spark = SparkSession.builder()
      .appName("sql")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse") //一定要这句，具体文件要创建
      .getOrCreate()
    import spark.implicits._  //导入隐式转换，RDD就可以转换成dataset,dataframe等
    val df: DataFrame = spark.read
        .option("header",value = true)  //设置数据第一行为 header
        .csv("")
    //查看 dataframe 的schema信息，要意识到dataframe是有结构信息的，叫schema
    df.printSchema()

    /**
     * 1.选择列
     * 2.过滤 NA 的 PM值
     * 3.分组求值
     */
    df.select('year,'month,'PM)
      .where('PM =!="NA")
      .groupBy('year,'month)
      .count()
      .show()

    //直接使用sql语句进行查询
    // 将dataframe注册为临表
    df.createOrReplaceTempView("pm")

    val result = spark.sql("select year,month,count(PM) from pm where PM!='NA' group by year,month")
    result.show()
  }

  /**
   * dataframe和dataset的区别
   * dataframe是弱类型的，编译不安全
   * dataset是强类型的，编译安全
   * dataframe就是dataset【Row】
   * dataframe=Row+schema
   */

  @Test
  def row(): Unit ={
    //row如何创建，它是什么
    //row对象必须配合schema对象才会有列名
   val zs = Person("zs",15)
    val row = Row(zs,15)
    //从row中获取数据
    row.getString(0)
    row.getInt(1)
    //row也是样例类
    row match {
      case Row(name,age)=>println(name,age)
    }
  }

}
case class Person(name:String,age:Int)  //数据结构对象一定要放最外面