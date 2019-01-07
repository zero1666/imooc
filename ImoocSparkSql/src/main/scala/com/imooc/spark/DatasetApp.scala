package com.imooc.spark

import org.apache.spark.sql.SparkSession

object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DatasetApp")
      .master("local[2]")
      .getOrCreate()

    val path = "/Users/david/workspace/spark_proj/ImoocSparkSql/sales.csv"

    // spark 读取csv文件
    val df = spark.read.option("header", "true").option("inferSchema","true").csv(path)
    df.show()

    //// Dataframe --> Dataset
    // 需要导入隐式转换
    import spark.implicits._
    val ds = df.as[Sales]  // Dataframe转换为 DataSet， Sales为指定的转换类型

    ds.map(line=>line.custom).show()


  }
  case class Sales(trans:Int, custom:Int, item:Int, amouint:Double)
}
