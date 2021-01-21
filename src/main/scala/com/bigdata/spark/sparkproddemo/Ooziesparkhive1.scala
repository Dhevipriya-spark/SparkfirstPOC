package com.bigdata.spark.sparkproddemo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Ooziesparkhive1 {
  def main(args: Array[String]) {

    // dont forget to use enablehivesupport if u want to store in hive
    val spark = SparkSession.builder.master("local[*]").appName("sparkhive").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = args(0)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res= spark.sql("select * from tab where state='NY'")
    res.show(5)
    val tab = args(1)
    val msurl="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    res.write.mode(SaveMode.Overwrite).jdbc(msurl,tab,msprop)
    res.write.mode(SaveMode.Overwrite).saveAsTable(tab) // store data in hive
    res.printSchema()
    spark.stop()
  }
}

//oozie job -info 0000031-210119011808783-oozie-oozi-C@spark-node
// spark-submit --class com.bigdata.spark.sparkproddemo.sparkhive --master local --deploy-mode client s3://dhevipriya/Appjar/sparkfirst_2.11-0.1.jar s3://dhevipriya/input/us-500.csv us500tab