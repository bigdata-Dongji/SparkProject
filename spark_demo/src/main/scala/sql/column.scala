package sql

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset, SparkSession}
import org.junit.Test

class column {

  val spark=SparkSession.builder()
    .appName("column")
    .master("local")
    .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
    .getOrCreate()
  import spark.implicits._

  /**
   * 创建column
   */
  @Test
  def creation(): Unit ={
    val ds: Dataset[(String, Int)] =Seq(("zhansan",10),("lisi",20)).toDS()
    val ds1: Dataset[(String, Int)] =Seq(("zhansan",10),("lisi",20)).toDS()
    val df: DataFrame =Seq(("zhansan",10),("lisi",20)).toDF("name","age")
    // ' 必须导入spark的隐式转换才能使用
    val col1: Symbol ='name
    // $ 必须导入spark的隐式转换才能使用
    val col2: ColumnName =$"name"
    // col 必须导入funtions
    import org.apache.spark.sql.functions._
    val col3: Column =col("name")
    // column 必须导入funtions
    val col4: Column =column("name")

    //上面的四种创建方式，有关联的dataset吗？
    ds.select('_1).show()
    //dataset可以，dataframe可以使用column对象选中行吗？可以
    df.select(col3).show()
    //select方法可以使用column对象选中列，那么其他算子呢？可以
    df.where(col2==="lisi").show()

    //dataset.col
    //使用dataset.col获取column对象，会和某个dataset进行绑定，在逻辑计划中，就会有不同表现，
    // 使用这个绑定的column对象查询别的dataset就会报错
    val column1: Column = ds.col("_1")
    val column2: Column = ds1.col("_1")

//    ds.select(column2).show //报错

    //为什么要和dataset绑定呢？方便join
//    ds.join(ds1,ds.col("_1")===ds1.col("_1"))

    //dataset.apply
    val column3: Column = ds.apply("name")
    val column4: Column = ds("name")
  }

  //as取别名
  @Test
  def as(): Unit ={
    val ds: Dataset[(String, Int)] =Seq(("zhansan",10),("lisi",20)).toDS()
    //select name,count(age) as age from table group by name
    ds.select('_1 as 'name).show()//创建别名
    ds.select('_2.as[Long]).show()//转换类型
  }

  @Test
  def columnapi(): Unit ={
    val ds: Dataset[(String, Int)] =Seq(("zhansan",10),("lisi",20)).toDS()
    //需求一：增加列，双倍年龄
    /*
    '_2*2其实本质是将一个表达式（逻辑执行计划）附着到column对象上
    表达式在执行的时候对应每一条数据进行操作
     */
    ds.withColumn("doubleage",'_2*2).show()
    //需求二：模糊查询
    ds.where('_1 like "z%").show()
    //需求三：排序，正反序
    ds.sort('age asc).show()
    //需求四：枚举判断
    ds.where('name isin("lisi","asda","sada")).show()
  }
}
