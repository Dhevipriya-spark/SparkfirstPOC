package com.bigdata.spark.sparkpractice

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object spark_transformationandaction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("spark_transformationandaction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}
