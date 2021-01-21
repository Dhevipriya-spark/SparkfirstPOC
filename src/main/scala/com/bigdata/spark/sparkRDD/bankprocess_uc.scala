package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object bankprocess_uc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bankprocess_uc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val bankrdd=sc.textFile("D:\\BigData\\Datasets\\bank-full.csv")
    val skip=bankrdd.first
    val res=bankrdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"", "")).map(x=>x.split(";"))
   .map(x=>(x(0).toInt,x(1),x(2),x(3),x(4),x(5).toInt,x(6),x(7),x(8),x(9).toInt,x(10),x(11).toInt,x(12).toInt,x(13).toInt)).filter(x=>x._6>50000)
    res.collect.foreach(println)
    spark.stop()
  }
}
