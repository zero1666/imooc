package com.imooc.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/*
create table day_video_access_topn_stat (
day varchar(8) not null,
cms_id bigint(10) not null,
times bigint(10) not null,
primary key (day, cms_id)
);


create table day_video_city_access_topn_stat (
day varchar(8) not null,
cms_id bigint(10) not null,
city varchar(20) not null,
times bigint(10) not null,
times_rank int not null,
primary key (day, cms_id, city)
);

create table day_video_traffics_topn_stat (
day varchar(8) not null,
cms_id bigint(10) not null,
traffics bigint(20) not null,
primary key (day, cms_id)
);

*
* */

/*
*
* TopN 统计 spark作业
* */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TopNStatJon")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]")
      .getOrCreate()



    val accessDF = spark.read.format("parquet").load("/Users/david/Downloads/data/access")
    // accessDF.printSchema()
    //accessDF.show(false)

    val day = "20170511"
    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF, day)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, accessDF, day)

    spark.stop()


  }

   /*
   * 最受欢迎的TopN课程
   * */
  def videoAccessTopNStat(spark:SparkSession, accessDF: DataFrame, day:String) ={


    // DataFrame 的方式
    /*
    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times"))
      .orderBy($"times".desc )

    videoAccessTopNDF.show(false)
    */

    // sql 方式           
    accessDF.createOrReplaceTempView("access_logs")
    /*
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day= " + day +  "  and cmsType='video' " +
      "group by day, cmsId order by times desc")
    */
    val videoAccessTopNDF = spark.sql(s"select day, cmsId, count(1) as times from access_logs where day= $day  and cmsType='video' group by day, cmsId order by times desc")
    videoAccessTopNDF.show(false)

    /*
    *  将数据写回到mysql表
    * */

  try{
    videoAccessTopNDF.foreachPartition(partitionOfRecords => {
      val list = new ListBuffer[DayVideoAccessStat]
      partitionOfRecords.foreach(info =>{
        val day =info.getAs[String]("day")
        val cmsId = info.getAs[Long]("cmsId")
        val times = info.getAs[Long]("times")
        list.append(DayVideoAccessStat(day, cmsId, times))
      })
      StatDAO.insertDayVideoAccessTopN(list)
    })
  }catch {
    case e:Exception => e.printStackTrace()
  }


  }

  def cityAccessTopNStat(spark:SparkSession, accessDF: DataFrame, day:String) ={


    // DataFrame 的方式

    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy($"day", $"city", $"cmsId").agg(count( "cmsId").as("times"))

   // cityAccessTopNDF.show(false)

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
      .orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <=3")

    //top3DF.orderBy("city")show(false)
    /*
    *  将数据写回到mysql表
    * */

    try{
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info =>{
          val day =info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }

  }


  /*
    * 按照流量进行统计
   */
  def videoTrafficsTopNStat(spark:SparkSession, accessDF: DataFrame, day:String) ={


    // DataFrame 的方式

    import spark.implicits._
    val trafficAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
        .groupBy("day","cmsId").agg(sum("traffic").alias("traffics"))
        .orderBy($"traffics".desc)

    trafficAccessTopNDF.show(false)



    /*
    *  将数据写回到mysql表
    * */

    try{
      trafficAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayTrafficVideoAccessStat]
        partitionOfRecords.foreach(info =>{
          val day =info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffic = info.getAs[Long]("traffics")
          list.append(DayTrafficVideoAccessStat(day, cmsId, traffic))
        })
        StatDAO.insertDayTrafficVideoAccessTopN(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }


  }

}
