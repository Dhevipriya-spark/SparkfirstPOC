package com.bigdata.spark.SparkKafka

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object sparkTwitterAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkTwitterAnalysis").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val APIkey= "2ZtYgh9prGSSFRHHj2JOagqfm"
    val APIsecretkey= "vV7ebTfoZVT9s34GF8RaHNuwz7YUamN4adcBXrelJlaLnSLSwY"
    val Accesstoken = "181460431-O98O3vCJ7jI0uR8kixfLfGBHdufktOOMX4oYuStF"
    val Accesstokensecret ="mPSW1B5WE8F2B0rJvZNjoplhotifKbouDkODIJOAioOZn"
     //my application communicating  with twitter with the help of below keys
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    //val lines = ssc.socketTextStream("localhost", 9999)

    val searchFilter = "spark,hive,hbase,nifi,cassandra,kafka"
    // create dstream
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))
    // tweetStream.print()


    //processing
    tweetStream.foreachRDD { abc =>
      val spark = SparkSession.builder.config(abc.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = abc.map(x=>(x.getUser.getScreenName(), x.getText())).toDF("user","tweet").withColumn("dates",current_date())
      df.show(2,false)
     // df.createOrReplaceTempView("tab")
      //val res = spark.sql("select * from tab where tweets lik")
     val res = df.where($"tweet".like("spark") && $"tweet".contains("http"))
      res.show(false)

      //date column and partitionBy is used to get one month old data stored in respective partition and then compare live data and old data
      //res.write.format("parquet").partitionBy("dates").saveAsTable("Twittertab")
      //sqoop ...jab ...
      //udf

      //2021-jan-30.... 1 lakh
      //2021-jan-31 .... 59k
      //
      //021-feb-4...90k

    }
/*
    tweetStream.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //  val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")

      val df = x.map(x => (x.getText(), x.getUser().getScreenName(),x.getCreatedAt().getTime())).toDF("msg", "username","createdDate")

      // val df = spark.read.json(x).withColumn("newcol",explode($"results")).drop($"results").select($"nationality",$"seed",$"newcol.user.",$"newcol.user.location.",$"newcol.user.name.*").drop("location","name","picture")
      // df.write.mode(SaveMode.Append).jdbc(ourl,"nifitab",oprop)
      // df.show(false)
      df.printSchema()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where msg like '%https://%'")
      res.show(false)
      val path = "file:///C:\\work\\datasets\\output\\twitterdata"
      res.write.format("csv").option("header","true").save(path)
      res.write.format("org.apache.spark.sql.cassandra").option("keyspace","venuks").option("table","twitter").save()

      /*      val ourl ="jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
            val oprop = new java.util.Properties()
            oprop.setProperty("user","ousername")
            oprop.setProperty("password","opassword")
            oprop.setProperty("driver","oracle.jdbc.OracleDriver")
            res.write.mode(SaveMode.Append).jdbc(ourl,"tweets",oprop)*/
    }
*/
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}

/*
To get old one month before data..store it in hive with partition
  to get incremental data run sqoop with incremental option
  to get incremental data use can even create a udf in spark ===============

offset is not possible in live data
kafka it is possible to do live processing and incremental data with below option or store with date based column(partition)
The minimum age of a log file to be eligible for deletion due to age
log.retention.hours=168
 */