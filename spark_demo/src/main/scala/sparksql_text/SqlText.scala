package sparksql_text

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{count, _}

import scala.collection.mutable

/**
 * HIVE题练习
 */
object SqlText {
  def main(args: Array[String]): Unit = {
    //创建SparkSession，sparksql入口
    val spark = SparkSession.builder()
      .appName("text1")
      .master("local")
      .config("spark.sql.warehouse.dir","G:\\ideaproject\\spark_demo\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    //读取数据文件
    val course = spark.read
      .csv("G:\\QQ\\2020省赛\\hive-data\\course.csv")
      .toDF("course_id","course_name","teacher_id")

    val scores = spark.read
      .csv("G:\\QQ\\2020省赛\\hive-data\\score.csv")
      .toDF("student_id","course_id","score")

    val score = scores.withColumn("score",'score.cast(IntegerType))

    val student = spark.read
      .csv("G:\\QQ\\2020省赛\\hive-data\\student.csv")
      .toDF("student_id","student_name","birthdate","sex")

    val teacher = spark.read
      .csv("G:\\QQ\\2020省赛\\hive-data\\teacher.csv")
      .toDF("teacher_id","teacher_name")

    // 查看表结构
    //    course.printSchema()
    //    score.printSchema()
    //    student.printSchema()
    //    teacher.printSchema()

    //注册临时表
    course.createTempView("course")
    score.createTempView("score")
    student.createTempView("student")
    teacher.createTempView("teacher")

    //    ①　查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    spark.sql(
      """
        |select d.*,s.score as 01_score,c.score as 02_score
        |from score as s join student d on s.course_id="01" join score c on c.course_id="02"
        |where s.student_id=c.student_id and s.student_id=d.student_id and s.score>c.score
        |""".stripMargin).show()
    score.as("s1").join(score.as("s2"),"student_id")
        .filter("s1.course_id='01' and s2.course_id='02' and s1.score>s2.score")
        .join(student,"student_id").show()
    //    ②　查询"01"课程比"02"课程成绩低的学生的信息及课程分数
    spark.sql(
      """
        |select d.*,s.score as 01_score,c.score as 02_score
        |from score as s join student d on s.course_id="01" join score c on c.course_id="02"
        |where s.student_id=c.student_id and s.student_id=d.student_id and s.score<c.score
        |""".stripMargin).show()
    score.as("s1").join(score.as("s2"),"student_id")
      .filter("s1.course_id='01' and s2.course_id='02' and s1.score<s2.score")
      .join(student,"student_id").show()
    //    ③　查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
    spark.sql(
      """
        |select d.student_id,d.student_name,avg(s.score) as num
        |from score as s join student d on s.student_id=d.student_id
        |group by d.student_id,d.student_name
        |having num>=60
        |""".stripMargin).show()
    score.join(student,"student_id").groupBy("student_id","student_name")
      .agg(avg("score").as("num"))
      .where('num>=60)
      .select("student_id","student_name","num").show()
    //    ④　查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩(包括有成绩的和无成绩的)
    spark.sql(
      """
        |select d.student_id,d.student_name,avg(s.score) as num
        |from score as s join student d on s.student_id=d.student_id
        |group by d.student_id,d.student_name
        |having num<60
        |""".stripMargin).show()
    score.join(student,Seq("student_id"),"right_outer").groupBy("student_id","student_name")
      .agg(avg("score").as("num"))
      .where('num<60 || 'num.isNull)
      .select("student_id","student_name","num").show()
    //    ⑤　查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
    spark.sql(
      """
        |select d.student_id,d.student_name,sum(s.score) as num,count(s.course_id)
        |from score as s join student d on s.student_id=d.student_id
        |group by d.student_id,d.student_name
        |""".stripMargin).show()
    score.join(student,"student_id").groupBy("student_id","student_name")
      .agg(sum("score").as("total"),count("course_id").as("num"))
      .select("student_id","student_name","total","num").show()
    //    ⑥　查询"李"姓老师的数量
    spark.sql(
      """
        |select count(*)
        |from teacher
        |where teacher_name like "李%"
        |""".stripMargin).show()
    teacher.groupBy('teacher_name).agg(count("teacher_name").as("num"))
    .where('teacher_name.like("李%")).select('teacher_name,'num).show()
    //    ⑦　查询学过"张三"老师授课的同学的信息
    spark.sql(
      """
        |select d.*,teacher_name
        |from teacher a join course b on a.teacher_id=b.teacher_id join score c on b.course_id=c.course_id join student d on c.student_id=d.student_id
        |where teacher_name="张三"
        |""".stripMargin).show()
    teacher.join(course,"teacher_id").join(score,"course_id")
      .join(student,"student_id")
      .where('teacher_name==="张三")
      .select("student_id","student_name","course_id","course_name","teacher_name").show()
    //******************************************************************************************************
    //    ⑧　查询没学过"张三"老师授课的同学的信息
    spark.sql(
      """
        |select d.*
        |from student d
        |  where d.student_id not in
        |(select c.student_id id
        |from teacher a join course b on a.teacher_id=b.teacher_id join score c on b.course_id=c.course_id
        |where teacher_name="张三")
        |""".stripMargin).show()
    //第一种方法：先查出学过张三老师课的学生id
    val rdd1: Dataset[Row] = teacher.join(course, "teacher_id")
      .join(score, "course_id")
      .select("student_id")
      .where('teacher_name === "张三")
    //TODO 很重要，Dataset[Row] 转 list
    val list1: List[String] = getTableFiledArray(rdd1.select("student_id")).toList
    //查看源码： list:_*代表全部元素，只写list不行；不存在not in 和 in，可以用 isin 和 ！ isin
    student.select('student_name,'student_id).where(!'student_id.isin(list1:_*)).show()
    //******************************************************************************************************
    //    ⑨　查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
    score.as("s1").join(score.as("s2"),"student_id")
        .where("s1.course_id='01' and s2.course_id='02'")
        .join(student,"student_id").show()
    //    ⑩　查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
    student.join(
      //没学过"02"课程的学生
      score.where("course_id = '02'"),Seq("student_id"),"left_outer")
      .as("s2")
      .where("s2.course_id is null")
      .join(
        //学过"01"课程的学生
        score.where("course_id = '01'"),"student_id")
      .show()
    //    11　查询没有学全所有课程的同学的信息
    student.join(
      //学生选课数：
      score.groupBy('student_id).count().as("s1"),Seq("student_id"),"left_outer"
    ).where(
      //总课程数：course.select("course_id").count     没上课的同学：s1.count is null
      s"s1.count!=${course.select("course_id").count} or s1.count is null"
    ).show()
    //    12　查询至少有一门课与学号为"01"的同学所学相同的同学的信息
    student.join(score,"student_id")
      .join(
        //学号为"01"的同学所学课
      score.select('course_id).where('student_id==="01"),"course_id")
      .select("student_id","student_name").distinct()
      .where("student_id !='01'").show()
    //    13　查询和"01"号的同学学习的课程完全相同的其他同学的信息
    score.groupBy('student_id).agg(concat_ws(",",collect_set('course_id)).as("course"))
        .where('student_id=!="01").as("a")//不是01学生的同学学习课程
    .join(
      //连接01学生的学习课程，连接点就是学习课程，既学习课程完全相同的
      score.groupBy('student_id).agg(concat_ws(",",collect_set('course_id)).as("course"))
        .where('student_id==="01").as("b"),"course"
    ).join(student,"student_id").show()
    //    14　查询没学过"张三"老师讲授的任一门课程的学生姓名
    //见上 8
    //    15　查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
    score.where('score<60).groupBy('student_id).count().where("count>=2")
      .join(score,"student_id").groupBy('student_id).avg("score")
      .join(student,"student_id").show()
    //    16　检索"01"课程分数小于60，按分数降序排列的学生信息
    score.where("course_id=='01' and score<60").join(student,"student_id")
      .orderBy('score.desc).show()
    //    17　按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    score.groupBy('student_id).agg(avg('score).as("num"))
      .join(score,"student_id").join(student,"student_id").show()
    //    18　查询各科成绩最高分、最低分和平均分：以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率:
    //    –及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
    score.groupBy("course_id").agg(
      max('score),min('score),avg('score),
      (sum(when('score>=60,1).otherwise(0))/count('course_id)).as("passrate"),
      (sum(when('score>=70&&'score<80,1).otherwise(0))/count('course_id)).as("mediumrate"),
      (sum(when('score>=80&&'score<90,1).otherwise(0))/count('course_id)).as("excellentrate"),
      (sum(when('score>=90,1).otherwise(0))/count('course_id)).as("excellencerate"))
      .join(course,"course_id")
      .show()
    //    19　按各科成绩进行排序，并显示排名
    score.select('*,
      row_number() over Window.partitionBy("course_id").orderBy('score.desc) as "rank")
      .join(student,"student_id")
      .show()
    //    20　查询学生的总成绩并进行排名
    score.groupBy('student_id).agg(sum('score).as("total"))
      .select('*,row_number() over Window.orderBy('total.desc) as "rank")
      .join(student,"student_id").show()
    //    21　查询不同老师所教不同课程平均分从高到低显示
    teacher.join(course,"teacher_id").join(score,"course_id")
      .groupBy('teacher_name,'course_name).agg(avg('score).as("avgscore"))
      .orderBy('avgscore.desc).show()
    //    22　查询所有课程的成绩第2名到第3名的学生信息及该课程成绩
    score.select('*,
      row_number() over Window.partitionBy("course_id").orderBy('score.desc) as "rank")
      .where('rank===2||'rank===3)
      .join(student,"student_id").show()
    //    23　统计各科成绩各分数段人数：课程编号,课程名称,[100-85],[85-70],[70-60],[0-60]及所占百分比
    score.groupBy('course_id).agg(
        (count(when('score>=0&&'score<60,1).otherwise(0))/count('student_id)).as("0-60"),
        (count(when('score>=60&&'score<70,1).otherwise(0))/count('student_id)).as("70-60"),
        (count(when('score>=70&&'score<85,1).otherwise(0))/count('student_id)).as("85-70"),
        (count(when('score>=85&&'score<100,1).otherwise(0))/count('student_id)).as("100-85"))
      .join(course,"course_id")
      .show()
    //    24　查询学生平均成绩及其名次
    score.groupBy('student_id).agg(avg('score).as("num"))
      .select('*,row_number() over Window.orderBy('num.desc) as "rank")
      .join(student,"student_id")
      .show()
    //    25　查询各科成绩前三名的成绩记录
    score.select('*,
      row_number() over Window.partitionBy('course_id).orderBy('score) as "rank")
      .where('rank<=3).join(student,"student_id").show()
    //    26　查询每门课程被选修的学生数
    score.groupBy('course_id).agg(count('student_id).as("num"))
      .join(course,"course_id").show()
    //    27　查询出只有两门课程的全部学生的学号和姓名
    score.groupBy('student_id).agg(count('course_id).as("num"))
      .where('num===2)
      .join(student,"student_id").show()
    //    28　查询男生、女生人数
    student.groupBy('sex).agg(count('student_id)).show()
    //    29　查询名字中含有"风"字的学生信息
    student.where('student_name like("%风%")).show()
    //    30　查询同名同性学生名单，并统计同名人数
    student.groupBy('student_name).agg(count('student_id).as("num"))
      .where('num>1).show()
    //    31　查询1990年出生的学生名单
    student.where(year('birthdate)===1990).show()
    //    32　查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
    score.groupBy('course_id).agg(avg('score).as("num"))
      .orderBy('num.desc,'course_id.asc).show()
    //    33　查询平均成绩大于等于85的所有学生的学号、姓名和平均成绩
    score.groupBy('student_id).agg(avg('score).as("num"))
      .where('num>=85).join(student,"student_id").show()
    //    34　查询课程名称为"数学"，且分数低于60的学生姓名和分数
    score.join(course,"course_id").where('course_name==="数学"&&'score<60)
      .join(student,"student_id").show()
    //    35　查询所有学生的课程及分数情况
    score.join(course,"course_id").join(student,"student_id").show()
    //    36　查询任何一门课程成绩在70分以上的学生姓名、课程名称和分数
    score.join(course,"course_id").join(student,"student_id")
      .where('score>70).show()
    //    37　查询课程不及格的学生
    score.where('score<60).join(student,"student_id").show()
    //    38　查询课程编号为01且课程成绩在80分以上的/学生的学号和姓名
    score.where('score>80&&'course_id==="01").show()
    //    39　求每门课程的学生人数
    score.groupBy('course_id).agg(count('student_id)).show()
    //    40　查询选修"张三"老师所授课程的学生中，成绩最高的学生信息及其成绩
    teacher.where('teacher_name==="张三").join(course,"teacher_id")
      .join(score,"course_id").orderBy('score.desc).limit(1).show()
    //    41　查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
    score.as("s1").join(score.as("s2"),"student_id")
      .where("s1.course_id!=s2.course_id and s1.score=s2.score")
      .distinct().show()
    //    42　查询每门课程成绩最好的前三名学生
    score.select('*,row_number() over Window.partitionBy('course_id).orderBy('score.desc) as "rank")
      .where('rank<=3).show()
    //    43　统计每门课程的学生选修人数（超过5人的课程才统计）:
    //    要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序排列
    score.groupBy('course_id).agg(count('student_id).as("num"))
      .where('num>5).orderBy('num.desc,'course_id.asc).show()
    //    44　检索至少选修两门课程的学生学号
    score.groupBy('student_id).agg(count('course_id).as("num"))
      .where('num>=2).show()
    //    45　查询选修了全部课程的学生信息
    score.groupBy('student_id).agg(concat_ws(",",collect_set('course_id)).as("course"))
      .join(
        course.select(concat_ws(",",collect_set('course_id)).as("course")),"course"
      ).join(student,"student_id").show()
    //    46　查询各学生的年龄(周岁):按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一
    student.select('*,year(current_date())-year('birthdate)).show()
    //    47　查询本周过生日的学生
    student.select('*).where(weekofyear(current_date())===weekofyear('birthdate)).show()
    //    48　查询下周过生日的学生
    student.select('*).where(weekofyear(current_date())+1===weekofyear('birthdate)).show()
    //    49　查询本月过生日的学生
    student.select('*).where(month(current_date())===month('birthdate)).show()
    //    50　查询12月份过生日的学生
    student.select('*).where(month('birthdate)===12).show()
  }
  def getTableFiledArray(t :DataFrame): mutable.Seq[String] = {
    var returnTableFiledArray:mutable.Seq[String] =  t.select("student_id")
      .as(Encoders.STRING).collect() // Dataset[Row] ==> mutable.Buffer[String]
    returnTableFiledArray
  }
}
