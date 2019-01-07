package com.imooc.spark


import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/*
*
*  访问日志转换(输入==>输出)工具类
* */
object AccessConvertUtil {
  /*定义输出字段*/
  val struct = types.StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )
  def parseLog(log:String) ={
    try{
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if(cmsTypeId.length > 1){
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      //
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    }
    catch{
      case e:Exception => Row(0)
    }



  }
}
