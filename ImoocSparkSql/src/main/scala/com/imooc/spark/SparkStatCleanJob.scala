package com.imooc.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkStatCleanJob")
      .getOrCreate()

    val accessRDD = spark.sparkContext.textFile("/Users/david/Downloads/data/access.log")
    //accessRDD.take(10).foreach(println)

    //RDD ==> DataFrame
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)
    //access.printSchema()
    //access.show(10, false)
    accessDF.coalesce(1).write.format("parquet").partitionBy("day")
      .mode(SaveMode.Overwrite).save("/Users/david/Downloads/data/access/")

    spark.stop()
  }
}
