package com.bigdata.spark.SparkKafka

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
/*
ZK is used to prevent DataLoss & toprovide high availability
Change values in  server.properties  in location D:\BigData\kafka_2.11-2.4.0\config
****Data is stored for 7 working days by default. change log.retention.hours to change the retention period
**** log.dirs=/tmp/kafka-logs ---A comma separated list of directories under which to store log files
log.dirs=/tmp/kafka-logs
 */
object kafkaconsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkkafka").getOrCreate()
     val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //10 sec old data ..microbatch processing
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic1", "topic2")
    // this stream ... kafkautils ... get data from kafka brokers next create a Dstream basedon ssc, and topics and kakfa config
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //create dstream
    val lines= stream.map(x => x.value) //Dstream createD
    //  lines.print()
   lines.foreachRDD{abc=>
      val spark = SparkSession.builder.config(abc.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val url ="jdbc:oracle:thin:@//bharathidb.c4nevuk0looq.us-east-2.rds.amazonaws.com:1521/ORCL"
      val prop = new  java.util.Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
      // Convert RDD[String] to DataFrame
      val df = abc.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.createOrReplaceTempView("tab")
      df.show()
      val mas = spark.sql("select * from tab where city='mas'")
      val tpj = spark.sql("select * from tab where city='tpj'")
      mas.write.mode(SaveMode.Append).jdbc(url,"chninfo1",prop)
      tpj.write.mode(SaveMode.Append).jdbc(url,"tpjinfo1",prop)

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
