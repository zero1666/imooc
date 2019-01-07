package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

object SQLContextApp {
  def main(args: Array[String]): Unit = {

    // 1) 创建响应的context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2)相关处理
    val path = args(0)
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3）关闭资源
    sc.stop()
  }
}
