package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/*
ReduceByKey..GroupByKey are implemented in key value pairs
ReduceBykey  =>processes only values ..sum up values of same keys
//only reduced no of records are transferred for further reduction
//In case of Reducebykey the transformations occur in memory and when reducebykey is encountered the
// reducebykey data of all the partitions is transferred  to a single local node and reduction happens in local filesystem before it returns the results. before transferring/shuffling the data
 merging happens locally on each mapper before sending results to a reducer(local filesystem), similarly to a "combiner" in MapReduce
 Output will be hash-partitioned with the existing partitioner/ parallelism level and then goes back to memory if needed for further processing
//check jan 8 video ( After 56 mins)

Merge the values for each key using an associative and commutative reduce function. This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/ parallelism level.
Merge the values for each key using an associative and commutative reduce function, but return the results immediately to the master as a Map. This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce.
Merge the values for each key using an associative and commutative reduce function. This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.

 */
object reduceByKey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvfile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val csvdata = "D:\\BigData\\Datasets\\asl.csv"
    val csvrdd = sc.textFile(csvdata)
    val head = csvrdd.first
    val res = csvrdd.filter(x=>x!=head)
      .map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((x,y)=>x+y)
   //val resfil =csvrdd.filter(x=>!(x.contains("age")))
    //scala> val skip=csvrdd.first
    //skip: String = name,age,city,day
    //scala> val csvres=csvrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3))).map(x=>(x._3,1)).reduceByKey((x,y)=>(x+y))
    //scala> csvres.collect.foreach(println)
    //(blr,4)
    //(hyd,1)
    //(mas,1)
    res.collect.foreach(println)
    // def reduceByKey(func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    //Any thing ending with key,value that data must be key value pair format
    spark.stop()
  }
}
//reduceByKey ... based on key apply a logic on top of only value and it returns (key,value) as output
//reducekey -Based on the keys, it is grouping the value
/*
blr,1..a
blr,1..b... blr,2...a
b1r,1..b... blr,3
blr,1......blr,4


 */

