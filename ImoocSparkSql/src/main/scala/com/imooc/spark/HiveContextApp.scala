package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // 1) 创建响应的context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2)相关处理
    println(hiveContext.tableNames().toString)


    //3）关闭资源
    sc.stop()
  }

}
