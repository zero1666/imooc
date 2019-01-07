package com.imooc.spark

import org.apache.spark.sql.SparkSession
import com.imooc.spark.DateUtils

/*
*
* 第一步清洗：抽取指定列数据
* */

object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkStatrFormat")
      .master("local[2]")
      .getOrCreate()

    val access = spark.sparkContext.textFile("/Users/david/Downloads/data/access.20161111.log")
    access.take(2).foreach(println)
    access.map(line =>{
      val splits = line.split(" ")
      val ip =splits(0)
      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
        * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
        */
      val time = splits(3) + " " + splits(4) //time
      val url = splits(10).replace("\"", "")
      val traffic =splits(9)
      (ip, DateUtils.parse(time), url, traffic)

    }).saveAsTextFile("/Users/david/Downloads/data/access.output.log")
    spark.stop()
  }
}
