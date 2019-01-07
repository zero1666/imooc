package com.imooc.spark

import org.apache.spark.sql.SparkSession

/*
* DataFrame 基本API
* */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame API")
      .master("local[2]")
      .getOrCreate()

    // 将json文件加载成一个dataframe
    val peopleDF = spark.read.format("json").option("multiline", true)
      .load("file:///Users/david/workspace/spark_proj/ImoocSparkSql/people.json")

    // 输出 DataFrame对应的scheme信息
    peopleDF.printSchema()

    // 默认输出数据集的前20条数据
    peopleDF.show()

    // 查询某列所有的数据
    peopleDF.select("name").show()

    // 查询某几列所有的数据，并对列进行计算 select name, age+10 as age2 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age")+10).as("age2")).show()

    // 根据某一列的值进行过滤： select * from table where age>19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    // 根据某一列进行分组，然后再进行聚合操作 select age, conut(1) from table group by age
    peopleDF.groupBy("age").count().show()

  }
}
