package com.imooc.spark

import com.ggstar.util.ip.IpHelper

/*
*
* IP 解析工具类
* */
object IpUtils {
  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }
  def main(args: Array[String]): Unit = {
    print(getCity("58.30.15.255"))
  }
}
