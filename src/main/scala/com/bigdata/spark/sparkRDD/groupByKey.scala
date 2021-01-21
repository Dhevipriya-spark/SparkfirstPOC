package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/*
//sbt -->Simple build tool
//GroupBykey
All keyvalue pairs are shuffled for further reduction
In Case of groupbykey, no merging happens locally on each mapper before sending results to a reducer so larger amount data is shuffled which reduces the performance
so reduceByKey is preferred as compared to GroupBykey

//Even though there is large amount of data is shuffled,to optimize the performance spark internally uses compactBuffer.Similar to Array buffer in java
 it is an alternative to ArrayBuffer that results in better performance because it allocates less memory
-- optimises little bit...compact buffer compresses shuffling related things.
**
* An append-only buffer similar to ArrayBuffer, but more memory-efficient for small buffers.
* ArrayBuffer always allocates an Object array to store the data, with 16 entries by default,
* so it has about 80-100 bytes of overhead. In contrast, CompactBuffer can keep up to two
* elements in fields of the main object, and only allocates an Array[AnyRef] if there are more
* entries than that. This makes it more efficient for operations like groupBy where we expect
* some keys to have very few elements.
//groupbykey
Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting
RDD with the existing partitioner/parallelism level. The ordering of elements within each group
is not guaranteed, and may even differ each time the resulting RDD is evaluated.
Note:
This operation may be very expensive. If you are grouping in order to perform an aggregation
(such as a sum or average) over each key, using PairRDDFunctions.aggregateByKey or
 PairRDDFunctions.reduceByKey will provide much better performance.

 Combiner
*/
object groupByKey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("groupByKey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\BigData\\Datasets\\donation.txt"
    val output="D:\\Bigdata\\Datasets\\grpbyout"

    sc.setLogLevel("ERROR") //only output and error msgs gets displayed

    val rdd=sc.textFile(data)
    val head=rdd.first
    val grprdd=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).groupByKey()
    .map(x=>x._1+","+x._2)//String interpolation(it removes the output braces in a tuple (a,b) => a,b i.e a,b as output
//
     grprdd.collect.foreach(println)
    /*
    (anu,CompactBuffer(3000, 1000))
(kumar,CompactBuffer(4000))
(prakash,CompactBuffer(2000))
(satya,CompactBuffer(4000, 400))
(venu,CompactBuffer(2000, 600))
(roja,CompactBuffer(2000, 1000))
     */
    //Both reducebykey and groupbykey does same but larger amount of data is shuffled in case of groupbykey.Before transferring combines in map and then transfers the data in case of reduceBykey
    val grprdd1=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).groupByKey().map(x=>(x._1,x._2.sum))
    grprdd1.collect.foreach(println)
   // grprdd1.saveAsTextFile(output)

    //grprdd1.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)
    /*
(anu,4000)
(kumar,4000)
(prakash,2000)
(satya,4400)
(venu,2600)
(roja,3000)
     */
    spark.stop()
  }
}
/*
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]

  spark-submit --class com.bigdata.spark.sparkRDD.groupByKey --master local --deploy-mode client target\scala-2.11\sparkfirst_2.11-0.1.jar
 */