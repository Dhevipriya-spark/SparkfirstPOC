package com.bigdata.spark.DatabricksRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkTransfpractice1 {
  def main(args: Array[String]) {

    //Narrow.Each partition of the parent RDD is used by at most one partition of the child RDD
    //Wide:Multiple child RDD partition may depend on a single parent RDD partition
    val spark = SparkSession.builder.master("local[*]").appName("sparkTransfpractice").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //MAP :Return a new RDD  by applying a function to each element of this RDD
    val rdd1= spark.sparkContext.parallelize(Array("a","b","c"))
    val res=rdd1.map(x=>(x,1))
    println(rdd1.collect().mkString(", "))
    println(res.collect().mkString(", "))  //(a,1), (b,1), (c,1)
    println(res.collect().foreach(println))
    /* (a,1)
      (b,1)
      (c,1)
      () */

    //Filter:Return a new RDD containing only elements that satisfy a predicate
    val rdd2=spark.sparkContext.parallelize(Array(1,2,3,4,5))
    val res1=rdd2.filter(x=>x%2 ==1)
    println(rdd2.collect().mkString(", "))
    println(res1.collect().mkString(", "))  //1, 3, 5

    //Flatmap:Return a new RDD by first applying a function to all elements of this RDD,and then flattening the results.
    val res2=rdd2.flatMap(x=>Array(x,x*100,42))
    println(res2.collect().mkString(","))
    println(res2.collect.mkString(","))
    //1,100,42,2,200,42,3,300,42,4,400,42,5,500,42,()


   //GroupBy:Group the data in the original RDD.create pairs where the key is the output of a user function,and the value is all the items for which function yields this key.
   val rdd3=sc.parallelize(Array("John","Fred","Anna","James"))
    val res3=rdd3.groupBy(x=>x.charAt(0))
    println(res3.collect.foreach(println))
    /*(A,CompactBuffer(Anna))
    (J,CompactBuffer(John, James))
    (F,CompactBuffer(Fred))
    ()*/
   //GroupByKey:Groups the values for each key in the original RDD.Create a new pair where the original key corresponds to this collected group of values
    val rdd4=sc.parallelize(Array(('B',3),('B',2),('B',1),('A',3),('A',3),('A',3)))
    val res4=rdd4.groupByKey()
    println(res4.collect().mkString(","))
     //(A,CompactBuffer(3, 3, 3)),(B,CompactBuffer(3, 2, 1))

    //ReduceByKey vs GroupByKey
    val words=Array("one","two","two","three","three")
    val wordpairsRDD=sc.parallelize(words).map(word=>(word,1))
    val wordCountsWithReduce= wordpairsRDD.reduceByKey(_+_).collect
    val wordCountsWithGroup=wordpairsRDD.groupByKey().map(t=>(t._1,t._2.sum)).collect
    /*
  groupByKey:  Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with the existing partitioner/parallelism level. The ordering of elements within each group is not guaranteed, and may even differ each time the resulting RDD is evaluated.
Note:This operation may be very expensive. If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey will provide much better performance.

     */

  //Mappartitions:Returns a new RDD by applying a function to each partition of this RDD
  val maprdd=sc.parallelize(Array(1,2,3),2)
    def f(i:Iterator[Int])={(i.sum,42).productIterator}
     val mpres=maprdd.mapPartitions(f)
    //glom=>flattens the elements on the same partition
    val xout=maprdd.glom().collect()
    val yout=mpres.glom().collect()

  //Mappartitionswithindex:Returns a new RDD by applying a function to each partition of this RDD,while tracking the index of the original partition
    def f1(partitionIndex:Int,i:Iterator[Int]) ={
      (partitionIndex,i.sum).productIterator
    }
   val mpires= maprdd.mapPartitionsWithIndex(f1)
   val yout1=mpires.glom().collect()

  //Sample :Returns a new RDD containing a statistical sample of the original RDD
    val samprdd=sc.parallelize(Array(1,2,3,4,5))
    val samres=samprdd.sample(false,0.4)
    //omitting seed will yield a different output

   //Union:Returns a new RDD containing all items from 2 original RDDs.Duplicates are not culled union.
    val unirdd1=sc.parallelize(Array(1,2,3),2)
    val unirdd2=sc.parallelize(Array(3,4),1)
    val unires=unirdd1.union(unirdd2)
    val zOut=unires.glom().collect()

    //Join:Returns a new RDD containing all pairs of elements having the same key in the original RDDs
    val jrdd1=sc.parallelize(Array(("a",1),("b",2)))
    val jrdd2=sc.parallelize(Array(("a", 3), ("a", 4), ("b", 5)))
    val jres=jrdd1.join(jrdd2)

    //Distinct:Return a new RDD containing distinct items from the original RDD(omitting all duplicates)
    val disrdd=sc.parallelize(Array(1,2,3,3,4))
    val disres=disrdd.distinct()
    println(disres.collect().mkString(","))

    //Coalesce :Returns a new RDD which is reduced to a smaller number of partitions
    //Coalesce(numPartitions,shuffle=false)
    val coalrdd=sc.parallelize(Array(1,2,3,4,5),3)
    val coalres=coalrdd.coalesce(2)
    val coalin=coalrdd.glom().collect()
    val coalout=coalres.glom().collect()

    //KeyBy:Create a pair rdd,forming one pair of each item in the original rdd.The pair's key is calculated
    //from the value via a user supplied function
    val keyrdd=sc.parallelize(Array("John","Fred","Anna","James"))
    val keyres=keyrdd.keyBy(w=>w.charAt(0))
    println(keyres.collect().mkString(","))

    //PartitionBy:Return a new rdd with specified no of partitions,placing original items into the partition
    //returned by a user supplied function //partitionBy(numpartitions,partitioner=portable_hash)
    import org.apache.spark.Partitioner
    val pbyrdd=sc.parallelize(Array(('J',"James"),('F',"Fred"),('A',"Anna"),('J',"John")),3)
     val pbyres=pbyrdd.partitionBy(new Partitioner() {
       val numPartitions=2
       def getPartition(k:Any) ={
         if(k.asInstanceOf[Char] <'H') 0 else 1
       }
     })
    val pbyout=pbyres.glom().collect()

    //Zip:Return a new RDD containing pairs whose key is the item in the original RDD and whose value is that item's corresponding element (same partition,same index) in a second RDD
    //zip(otherRDD)
    val ziprdd1=sc.parallelize(Array(1,2,3))
    val ziprdd2=ziprdd1.map(n=>n*n)
    val zipout=ziprdd1.zip(ziprdd2)
    println(zipout.collect().mkString(","))


    spark.stop()
  }
}
