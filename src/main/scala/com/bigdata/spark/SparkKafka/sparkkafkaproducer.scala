package com.bigdata.spark.SparkKafka

import org.apache.kafka.clients.producer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
/*
this code main purpose,  server/app generate logs, that logs store in one file with a certain interval .
get that logs using kafka producer api or any other language ..

Read file using scala
https://alvinalexander.com/scala/how-to-open-read-text-files-in-scala-cookbook-examples/
To handle each line in the file as it’s read, use this approach:

import scala.io.Source

val filename = "fileopen.scala"
for (line <- Source.fromFile(filename).getLines) {
    println(line)
}


Read the file into a list or array
As a variation of this, use the following approach to get all of the lines from the file as a List or Array:

val lines = Source.fromFile("/Users/Al/.bash_profile").getLines.toList
val lines = Source.fromFile("/Users/Al/.bash_profile").getLines.toArray
 */
import scala.io.Source

object sparkkafkaproducer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkkafkaproducer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    import scala.util.Try
    //write any code (java, scala or spark) to get logs from path file
    //val path = args(0)
    val path = "D:\\BigData\\pyspark\\access_log_20210204-064502.log"
    val data = spark.sparkContext.textFile(path) //Read File using Spark
    // val data = Source.fromFile(path).getLines.toList // Read file using scala
    data.foreachPartition(y => {   //applies a function to each partition of this rdd
      import java.util._
  //For each partition of y(rdd), apply a function to all elements of the rdd (producer will send the code in format[ topic,data] thats why string encoder as [string,string]) key is topic valus is string
      val props = new java.util.Properties()
      props.put("metadata.broker.list", "localhost:9092") //kafka broker list
      props.put("serializer.class", "kafka.serializer.StringEncoder") //
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("bootstrap.servers", "localhost:9092")
  //producer is serialising the data and consumer is deserialising the data
      // import kafka.producer._
      // val config = new ProducerConfig(props)
      //producer code to send data( [String,String] to kafka servers based on props
      val producer = new KafkaProducer[String, String](props)
      val topic = "logslatest".toSeq
      y.foreach(x => {  //Applies a function f to all elements of this RDD.
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString())) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indeng,"anu,56,mas")
        Thread.sleep(5000)
        //kafka send million msg per seconds. but application sending 2 logs per 10 sec ... all logs send within fraction of milli seconds. so its not visible so
        //thats y if u mention Thread.sleep(5000) wait 5000 milli seconds (5 sec) next send logs


      })

    })


    ssc.start()
    ssc.awaitTermination()
  }
}

/*
Consumer should be running else producer will fail with below error
[2021-02-03 11:43:30,713] ERROR Error starting the context, marking it as stopped (org.apache.spark.streaming.StreamingContext:91)
java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute
	at scala.Predef$.require(Predef.scala:224)

 */