package com.bigdata.spark.sparkproddemo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OoziesparkHive2 {
  def main(args: Array[String]) {

    // dont forget to use enablehivesupport if u want to store in hive
    val spark = SparkSession.builder.master("local[*]").appName("OoziesqoopsparkHive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val tab = args(0)
    val msurl="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val res = spark.read.jdbc(msurl,tab,msprop)//read from mssql eg:dept

    //res.write.mode(SaveMode.Overwrite).jdbc(msurl,tab,msprop)
    res.write.mode(SaveMode.Append).saveAsTable(tab) // store dept table in hive
    res.printSchema()
    spark.stop()
  }
}
