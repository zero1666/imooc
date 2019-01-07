package com.imooc.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

/*
*
* Mysql 操作工具类
* */

object MySQLUtils {
  /*
  * 获取数据库连接
  * */
  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc?user=root&password=rootroot")
  }

  def release(connection: Connection, pstmt:PreparedStatement) = {
    try{
      if(pstmt != null){
        pstmt.close()
      }
    }catch{
      case e:Exception =>e.printStackTrace()
    }finally {
      if(connection != null){
        connection.close()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
