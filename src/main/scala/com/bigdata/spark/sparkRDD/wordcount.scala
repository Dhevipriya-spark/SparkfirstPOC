package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// To find Trending topics in twitter
//Flatmap is intentionally designed for unstructured data but can process structured data
//Hello world program of any bigdata technology
object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("flatmapuc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\bigdata\\datasets\\wcdata.txt"
    val urdd = sc.textFile(data)
   // val res1=urdd.map(x=>x.split(" "))
    val res = urdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    res.take(15).foreach(println)

    //urdd.collect.foreach(println)
    // collect : (Action)i want to call /display all records/lines use collect.
    // take :(Action) based on specified num display top n number of lines. if we have millions of rows.we cannot displaying allline..so take is always recommended
    //reduce by key .... if u want to group by value use reduceByKey ...its almost like select city, count(*) cnt from tab group by city..
    //Large data -take recommended so that we have a check at sample of the data too
    //Small data -Either collect or take
    //Flatmap -->use when there is no structure in data eg:when you want to find  most trending keyword in twitter .Twitter is unstructured,which has hashkey has been used the most
    //sqL -groupby is equivalent to reducebykey in sparksql
    //sortBy =>Return this RDD sorted by the given key function.
    spark.stop()
  }
}
/*
coins ...rdd
notes...df(extension of rdd)
apis ...dataset(extension of rdd and df)
 */