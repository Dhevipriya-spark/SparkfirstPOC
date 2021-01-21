package com.bigdata.spark.sparkproddemo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OozieS3toHive3 {
  //// spark-submit --class com.bigdata.spark.sparkproddemo.S3toHive   --master local --deploy-mode client s3://dhevipriya/Appjar/sparkfirst_2.11-0.1.jar s3://dhevipriya/input/us-500.csv us500tab
  def main(args: Array[String]) {
    // dont forget to use enablehivesupport if u want to store in hive
    val spark = SparkSession.builder.master("local[*]").appName("s3toHive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = args(0)
    val tab = args(1)
    val res = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

    res.write.mode(SaveMode.Overwrite).saveAsTable(tab) // store data in hive
    res.printSchema()


    spark.stop()
  }
}
