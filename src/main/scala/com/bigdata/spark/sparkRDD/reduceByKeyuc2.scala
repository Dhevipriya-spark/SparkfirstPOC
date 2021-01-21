package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql._

object reduceByKeyuc2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvfile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val csvdata = "D:\\bigdata\\datasets\\rbkmuldata.csv"
    val csvrdd = sc.textFile(csvdata)
    val head = csvrdd.first()
    val res = csvrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).map{case (k,v,v1)=> ((v,v1),(1,1))}
      .reduceByKey{ (a,b)=>(a._1+b._1,a._2+b._2)}
    res.foreach(println)
    // def reduceByKey(func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }


    spark.stop()
  }
}

//reduceByKey ... based on key apply a logic on top of value ...
/*
blr,1..a
blr,1..b... blr,2...a
b1r,1..b... blr,3
blr,1......blr,4

((ap,vij),1,1) a._1,a._2 //(a._1+b._1,a._2+b._2)
((ap,vij),1,1)  b._1,b._2 ==>>((ap,vij),1+1,1+1)  a._1=2,a._2=2
((ap,vij),1,1)  b._1,b._2 ==>((ap,vij),3,3)
 */
