package com.bigdata.spark.sparkproddemo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparksql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparksql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    //val data = "D:\\BigData\\Datasets\\bank-full.csv"
    val data = args(0) // To dynamically pass values to the program(Real time)

    val df1 = spark.read.format("csv").option("header","true").option("delimiter",";")
      .option("inferSchema","true").load(data)
    df1.createOrReplaceTempView("tab1")
    df1.printSchema

    val res = spark.sql("select * from tab1")
    res.show(5)

    val tab=args(1)
    val msurl="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    //res.write.mode(SaveMode.Overwrite).saveAsTable(msurl,tab1,msprop)
    res.write.jdbc(msurl,tab,msprop)
    //res.write.mode(SaveMode.Overwrite).jdbc(msurl,tab,msprop)

    res.printSchema()
    spark.stop()
  }
}
