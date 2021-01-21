package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//spark sql is recommended for performance due to catalyst optimizer(internally sql queries are converted rdd)
// but the below spark.sql(schemardd) is a old way doing it
//RDD- used for processing mainly unstructured
 //Dataframe api -Structured and semi structured data
//Dataframe supports many third partypackages i.e (spark-avro,spark-redshift,spark-als) which RDD does not support and doing it in rdd is very tedious too. so it is highly advantageous
object reduceByKeysparksql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvfile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    //implicits is used to convert rdd to dataframe or dataframe to dataset or rdd to dstream or dstream to rdd or dstream to df
    //when u want to convert from 1 api to another api ,implicits is used .toDF wont work if import statement is  not used
    import spark.implicits._
    val csvdata = "D:\\bigdata\\datasets\\rbkmuldata.csv"
    val csvrdd = sc.textFile(csvdata)
    val head = csvrdd.first()
    //This is SCHEMARDD(old way )
    //toDF method used convert structure rdd to dataframe and to rename column
    val res = csvrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","state","city")
    res.show()
    res.createOrReplaceTempView("tab")
    val result = spark.sql("select state, city, count(*) cnt from tab group by state, city order by cnt desc")
    //result: org.apache.spark.sql.DataFrame = [state: string, city: string ... 1 more field]
    result.show()
    //Programming model(Dataset)
    val result1=res.groupBy($"state",$"city").count().orderBy($"count".desc)
    //result1: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [state: string, city: string ... 1 more field]
    result1.show()
  // performance wise dataframe is highly preferred one

    //.map{case (k,v,v1)=> ((v,v1),(1,1))}
    //  .reduceByKey{ (a,b)=>(a._1+b._1,a._2+b._2)}

    // res.foreach(println)
    // def O(func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    spark.stop()
  }
}

//reduceByKey ... based on key apply a logic on top of value ...
//90% df/ds
/*
blr,1..a
blr,1..b... blr,2...a
b1r,1..b... blr,3
blr,1......blr,4

 */
