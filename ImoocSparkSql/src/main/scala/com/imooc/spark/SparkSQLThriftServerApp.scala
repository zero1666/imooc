package com.imooc.spark

import java.sql.DriverManager

/**
  * JDBC 访问 spark sql
*/

object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver") // 实例化driver
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000", "david", "")
    val pstmt = conn.prepareStatement("select username, age, city from people_test")
    val rs = pstmt.executeQuery()
    while(rs.next()){
      println("name:" + rs.getString("username") +
        ", age:" + rs.getInt("age") + ", city" +rs.getString("city"))
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}
