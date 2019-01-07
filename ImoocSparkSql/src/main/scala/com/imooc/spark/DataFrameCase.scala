package com.imooc.spark

import org.apache.spark.sql.SparkSession

/*
* DataFrame 中其它操作
* */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameCase")
      .master("local[2]")
      .getOrCreate()

    //RDD
    val rdd = spark.sparkContext.textFile("file:///Users/david/workspace/spark_proj/ImoocSparkSql/student.data")

    // 打印RDD内的数据，用于debug
    rdd.take(100).foreach(println)

    import spark.implicits._
    val studentDF = rdd.map(_.split(","))
      .map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //// 输出到控制台相关
    // DataFrame 打印scheme
    studentDF.printSchema()

    // show() 默认展示前20条
    println("test show()")
    studentDF.show()
    studentDF.show(30) //展示30条数据

    println("test takstudentDF.take(2).foreach(println)  // 返回前2条记录e()")
    studentDF.take(2).foreach(println)  // 返回前2条记录
    println("test first()")
    println(studentDF.first()) //  显示第一条记录

    ////  选择
    println("test select")
    //选取指定列, 注意指定多列的时候顺序不可颠倒，必须跟定义的列顺序一致
    studentDF.select("name","email").show()
    studentDF.select(studentDF("name").as("student_name")).show() // 修改列名

    // 过滤， 取指定值的列。这里是取name 为空或=NULL的列
    studentDF.filter("name='' OR name='NULL' ").show()
    studentDF.filter("SUBSTR(name, 0, 1)='B' ").show() // 取name开头第一个字母是B的列

    //排序
    studentDF.sort(studentDF("name")).show() //默认升序
    studentDF.sort(studentDF("name").desc).show() // 降序排
    studentDF.sort("name","id").show() // 多列排序
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show()

    //join
    val studentDF2 = rdd.map(_.split(","))
      .map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    println("test join")
    studentDF.join(studentDF2, studentDF.col("id") === studentDF2("id")).show()

    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
