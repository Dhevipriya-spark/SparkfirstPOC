package com.bigdata.spark.SparkKafka


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.functions._
/*
Nifi is not only data ingestion but a unified platform to manage workflow

Apache NiFi and Apache Kafka are two different tools with different use-cases that may
slightly overlap. Here is my understanding of the purpose of the two projects.

NiFi is "An easy to use, powerful, and reliable system to process and distribute data."

It is a visual tool (with a REST api) that implements flow-based programming to enable
the user to craft flows that will take data from a large variety of different sources,
perform enrichment, routing, etc on the data as it's being processed, and output the
result to a large variety of destinations. During this process, it captures metadata
(provenance) on what has happened to each piece of data (FlowFile) as it made its way
through the Flow for audit logging and troubleshooting purposes.

"Apache Kafka is publish-subscribe messaging rethought as a distributed commit log"

It is a distributed implementation of the publish-subscribe pattern that allows
developers to connect programs to each other in different languages and across a large
number of machines. It is more of a building block for distributed computing than it is
 an all-in-one solution for processing data

 */

//Nifi get data from rest api(invoke http) and store the data in kafka brokers(publishkafkarecord2.0)
object kafkaConsumerNifi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("kafkaConsumer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("nifi")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    // dstream created

    val lines=  stream.map(record =>  record.value)
   // lines.print()
   lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val df1 = spark.read.json(x)
      val df = df1.withColumn("t",explode($"results")).drop($"results")
        .select("nationality","seed","t.user.picture.*","t.user.name.*","t.user.*", "t.user.location.*")
        .drop("picture","name","location")

      df.createOrReplaceTempView("tab")
     // df.show
     //df.printSchema()
   val url ="jdbc:oracle:thin:@//bharathidb.c4nevuk0looq.us-east-2.rds.amazonaws.com:1521/ORCL"
      val prop = new  java.util.Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
      df.write.mode(SaveMode.Overwrite).jdbc(url,"nifidata1",prop) //As there is a new columns  we are giving savemode overwrite
      // val df = x.map(x => x.split(" ")).map(x => (x(0), x(3), x(4))).toDF("ip", "dt", "tz")

      // val df = spark.read.option("","true").json(x).withColumn("newcol",explode($"results")).drop($"results").select($"nationality",$"seed",$"newcol.user.",$"newcol.user.location.",$"newcol.user.name.*").drop("location","name","picture")
      //df.write.mode(SaveMode.Append).jdbc(ourl,"kafkalogs23dec",oprop)
      df.show()
      df.printSchema()
      df.createOrReplaceTempView("tab")
      //spark.sql("show tables;")
      //df.write.mode(SaveMode.Append).saveAsTable("hiv")


    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()

  }
}
