package com.bigdata.spark.DatabricksRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkActionPractice1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkActionPractice1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //distributed-occurs across the cluster
    //driver-result must fit in the driver JVM

    //getNumPartitions()- Returns the number of partitions in RDD
    val xrdd=sc.parallelize(Array(1,2,3),2)
    val xsize=xrdd.partitions.size
    val xout=xrdd.glom().collect()

    //collect()-Return all items in the RDD to the driver in a single list
    val yout=xrdd.collect()
    println(yout)


    //Reduce()-Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results,and returns the results to the driver.
     val redrdd=sc.parallelize(Array(1,2,3,4))
     val redout=redrdd.reduce(_+_)
     println(redrdd.collect.mkString(","))
     println(redout)

    //Aggregate()-Aggregate all the elements of the RDD by:
    //-applying a user function to combine elements with user-supplied objects
    //-then combining those user-defines results via a second use function
    //and finally returning a result to the driver

  /* def seqOp=(data:(Array[Int]),Int),item:Int)=> (data._1 :+ item,data._2 + item)
    def combOp= (d1:(Array[Int], Int), d2:(Array[Int], Int)) => (d1._1.union(d2._1), d1._2 + d2._2)

    val x= sc.parallelize(Array(1,2,3,4))
    val y= x.aggregate((Array[Int](), 0))(seqOp,combOp)
 */
    //max()->Return the maximum item in the RDD
    val maxrdd= sc.parallelize(Array(2,4,1))
    val maxout=maxrdd.max
    println(maxrdd.collect().mkString(","))
    println(maxout)

    //sum -> return the sum of the items in the rdd
    val sumrdd=sc.parallelize(Array(2,4,1))
    val sumout=sumrdd.sum
    println(sumrdd.collect().mkString(","))
    println(sumout)

    //Mean ->Return the mean of the items in the RDD.
    val meanrdd=sc.parallelize(Array(2,4,1))
    val meanout=meanrdd.mean
    println(meanrdd.collect().mkString(","))
    println(meanout)

    //stdev() ->Returns the std deviation of the items in the RDD.
    val stdrdd=sc.parallelize(Array(2,4,1))
    val stdout=stdrdd.stdev
    println(stdrdd.collect().mkString(","))

    //CountByKey ->Returns a map of keys and counts of thier occurences in the RDD.
    val countrdd=sc.parallelize(Array(('J',"James"),('F',"Fred"),('A',"Anna"),('J',"John")))
    val countout=countrdd.countByKey()
    println(countout)

    //SaveASTextFile->save the RDD to the filesystem indicated in the path
    val saverdd=sc.parallelize(Array(2,4,1))
    saverdd.saveAsTextFile("/temp/demo")
    val saveout=sc.textFile("/temp/demo")
    println(saveout.collect().mkString(","))

    spark.stop()
  }
}
