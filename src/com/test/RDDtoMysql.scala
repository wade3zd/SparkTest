package com.test

/**
  * Created by zhangdong on 2016/5/3.
  * Command:spark-submit --class com.test.RDDtoMysql  --jars /usr/local/spark/lib/mysql-connector-java-5.1.38-bin.jar /root/SparkTest.jar
  */
import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}


object RDDtoMysql {

  case class Blog(name: String, count: Int)

  def myFun(iterator: Iterator[(String, Double)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog2(name, count) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://101.201.149.190:3306/test","root", "1qazXSW@")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setDouble(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
}
    }
  }
/*
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(("www", 10), ("iteblog", 20), ("com", 30)))
    data.foreachPartition(myFun)
  }*/
}
