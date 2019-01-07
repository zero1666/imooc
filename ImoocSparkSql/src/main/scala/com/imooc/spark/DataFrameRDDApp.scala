package com.imooc.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object DataFrameRDDApp {
  // 方法1 根据包括case class数据的RDD转换成DataFrame
  // case class定义表的schema，case class的属性会被读取并且成为列的名字
  case class Info(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame RDD")
      .master("local[2]")
      .getOrCreate()

    // inferReflection(spark)
    program(spark)

    spark.stop()



  }

  def program(spark : SparkSession): Unit ={
    // RDD
    val rdd = spark.sparkContext.textFile("file:///Users/david/workspace/spark_proj/ImoocSparkSql/infos.txt")

    //step1: 从原来的 RDD 创建一个Row的 RDD
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    //step2: 创建由一个 StructType 表示的scheme 并且与第一步创建的 RDD 的行结构相匹配
    //构造schema用到了两个类StructType和StructField，其中StructField类的三个参数分别是(字段名称，类型，数据是否可以用null填充)
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    //step3.在Row RDD 上通过 createDataFrame 方法应用scheme
    val infoDF = spark.createDataFrame(infoRDD, structType)

    //然后可以对表进行各种操作
    infoDF.printSchema()
    infoDF.show()

    /// 基于DataFrame API操作
    infoDF.show()
    infoDF.filter(infoDF.col("age") > 18).show()

    /// 基于SQL API操作
    ////注册成一个表
    infoDF.createOrReplaceTempView("infos")
    //然后可以对表进行各种操作
    spark.sql("select * from infos where age > 18").show()

  }

  def inferReflection(spark: SparkSession): Unit ={
    // RDD
    val rdd = spark.sparkContext.textFile("file:///Users/david/workspace/spark_proj/ImoocSparkSql/infos.txt")

    // Note: 需要导入隐式转换
    import spark.implicits._
    //先将RDD转化成case class 数据类型，然后再通过toDF()方法隐式转换成DataFrame
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    /// 基于DataFrame API操作
    infoDF.show()
    infoDF.filter(infoDF.col("age") > 18).show()

    /// 基于SQL API操作
    ////注册成一个表
    infoDF.createOrReplaceTempView("infos")
    //然后可以对表进行各种操作
    spark.sql("select * from infos where age > 18").show()
  }


}
