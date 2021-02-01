package com.bigdata.spark.Streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object streamingwc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("streamingwc").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}
