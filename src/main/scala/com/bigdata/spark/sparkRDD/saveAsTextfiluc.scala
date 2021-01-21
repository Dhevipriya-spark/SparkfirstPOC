package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//spark-submit --class BIM.DEV.SPARKCORE.poc_t1_removeheader1 --master local --deploy-mode client target\scala-2.11\sparktest_2.11-0.1.jar
//map(
object saveAsTextfiluc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("saveAsTextfiluc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
      sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data = "D:\\BigData\\Datasets\\donation.txt"
    val output = "D:\\BigData\\Datasets\\output\\donationop"
     // .files are hidden files
    //By default spark creates 2 input partitions.i.e min partitions =2.No of partitions is equal to output partitions .so  no of output partitions is also 2
    //But if you are grouping in sparksql you will get 200 partitions
    //if with sc.textfile it has 6 partitions same no output partitions.so it will also have 6 output partitions .we can modify the output partitions in reducebykey or sortby transformation
    //if u have large amount of data. we split the data to number of files called partitions so as to increase the performance ..to improve parallelism ..we  increase the no of input partitions.
    //Not recommended to  use small or large no of partitions..suggested to use medium no of partitions.
    //By default not possible to write header to a output file .But we can do it with DF. toDF()
    //impilicits  is amust while converting from 1 api to another api (RDD to DF)
    import spark.implicits._
    val rdd = sc.textFile(data,6)
    val head = rdd.first()
    val res1=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1)))
   // res1.collect.foreach(println)
   /* (venu,2000)
    (satya,4000)
    (satya,400)
    (venu,600)
    (roja,2000)
    (roja,1000)
    (anu,3000)
    (kumar,4000)
    (prakash,2000)
    (anu,1000)*/
    val res2=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1))).reduceByKey(_+_)
    //res2.collect.foreach(println)
    /*(kumar,4000)
(anu,30001000)
(prakash,2000)
(satya,4000400)
(roja,20001000)
(venu,2000600)*/
    val res3=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).reduceByKey(_+_).sortBy(x=>x._2,false)
    res3.collect.foreach(println)

    //Dataframe ->orderBy
    //RDD->sortBy
    //sortBy-Return this RDD sorted by the given key function.
    val res = rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).reduceByKey((a,b)=>a+b,1).sortBy(x=>x._2,false).toDF("name","amt")
    res.write.format("csv").option("header","true").save(output)
    //to overwrite output data
    res.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)
    //res.saveAsTextFile(output)

   val newrdd=sc.makeRDD(List(1,2,3,4))
   val filterrdRDD=newrdd.filter(num=>num%2 != 0)
    filterrdRDD.collect().foreach(println)
    spark.stop()
  }
}
