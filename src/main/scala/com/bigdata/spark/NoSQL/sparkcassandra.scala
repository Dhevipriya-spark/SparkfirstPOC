package com.bigdata.spark.NoSQL

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkcassandra {
  def main(args: Array[String]) {
    //
    val spark = SparkSession.builder.master("local[*]").appName("sparkcassandra").config("spark.cassandra.connection.host","127.0.0.1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    /*val adf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","venudb").option("table","asl").load()
       val ndf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","venudb").option("table","nep").load()
        val join = adf.join(ndf,$"name"===$"name","inner")
        join.show()*/
    val asldf= spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "venudb")
      .option("table", "asl")
      .load()
    asldf.show()
    val nepdf= spark.read.format("org.apache.spark.sql.cassandra")
      .option("keyspace", "venudb").option("table", "nep").load()
    asldf.createOrReplaceTempView("asl")
    nepdf.createOrReplaceTempView("nep")
    val res = spark.sql("select a.*, n.phone, n.email from asl a join nep n on a.name=n.name")
    res.show()
    //create table structure  before
    res.write.format("org.apache.spark.sql.cassandra").option("keyspace", "venudb").option("table", "neaslnep").save()
    spark.stop()
  }
}

//spark-submit --class com.bigdata.spark.cassandra.sparkcassandra --master local --deploy-mode client  target\scala-2.11\sparkfirst_2.11-0.1.jar
