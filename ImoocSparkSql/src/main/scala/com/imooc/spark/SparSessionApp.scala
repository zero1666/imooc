package com.imooc.spark

import org.apache.spark.sql.{SQLContext, SparkSession}

object SparSessionApp {
  def main(args: Array[String]): Unit = {

    // 1) 创建响应的context
    val spark = SparkSession.builder()
      .appName("spark session example")
      .master("local[2]")
      .getOrCreate()


    //2)相关处理
    val path = args(0)
    val people = spark.read.json("file:///Users/david/workspace/spark_proj/ImoocSparkSql/people.json")
    people.show()

    //3) stop
    spark.stop()



  }
}