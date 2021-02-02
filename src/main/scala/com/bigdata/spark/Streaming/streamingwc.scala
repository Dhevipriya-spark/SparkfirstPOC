package com.bigdata.spark.Streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
/*
sparkContext used to create  RDD API
sqlContext  used to create dataframe
sparkSession used to create dataset.
sparkstreamingContext ..ssc used to create Dstream ...DstreamAPI is used to process streaming data,live data.


 You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using
 Netcat creates a dummy portno,dummy website.its used to create logs from this portno ..
 Testing purpose

 // Go to EC2 instance ,include custom tcp port  as  inbound security group
 .whenevr  your program not executing go to security group ..change portno and  then  reexecute

 Https ==>website
 */
object streamingwc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("streamingwc").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    import java.util.Properties

    //create a dstream ... imp
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("ec2-13-233-105-208.ap-south-1.compute.amazonaws.com", 9999)
   // lines.print()  //Print the results
   //nc -lk 999 -netcat server send this data to this port no .spark is receiving this data from port no of the webserver and listing the data
    lines.foreachRDD{abc=>
      val spark = SparkSession.builder.config(abc.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val url ="jdbc:oracle:thin:@//bharathidb.c4nevuk0looq.us-east-2.rds.amazonaws.com:1521/ORCL"
      val prop = new Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
      // Convert RDD[String] to DataFrame
      val df = abc.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.createOrReplaceTempView("tab")
      df.show()
      df.write.mode(SaveMode.Append).jdbc(url,"Dptest1",prop)

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    spark.stop()
  }
}
