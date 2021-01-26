package com.bigdata.spark.NoSQL

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkhbasephoenix {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkhbase").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

 /*   //Reading  table emp from  Apache pheonix
    val tab1 = args(0)
   val empdf = spark.read.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table",tab1).load()
    empdf.show()
*/
    //val data = "s3://dhevipriya2020/us-500.csv" //In class all jars and input files in s3.my s3 is full so using alternative
    val data ="s3://dhevipriya2020/us-500.csv"
   val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","usdata").save()
    df.show()

    spark.stop()
  }
}
/*
 spark-submit --class com.bigdata.spark.NoSQL.sparkhbasephoenix --master local --deploy-mode client /home/hadoop/sparkfirst_2.11-0.1.jar EMP
 spark-submit --class com.bigdata.spark.NoSQL.sparkhbasephoenix --master local --deploy-mode client  /home/hadoop/ EMP

Caused by: java.lang.ClassNotFoundException: org.apache.phoenix.spark.DefaultSource

in prod env multi framework integratio nintegration very headache
so copy jars to lib
sudo cp /usr/lib/phoenix/phoenix-spark-4.14.3-HBase-1.4.jar /usr/lib/spark/jars
sudo cp /usr/lib/phoenix/phoenix-4.14.3-HBase-1.4-client.jar /usr/lib/spark/jars
 // us-500.csv store in s3 next export to phoenix to process

 create table usdata (first_name varchar(32) primary key, last_name varchar(32), company_name varchar(32), address varchar(32), city varchar(32), county varchar(32), state varchar(32), zip integer, phone1 varchar(32), phone2 varchar(32), email varchar(64), web varchar(64))

 spark-submit --class com.bigdata.spark.NoSQL.sparkhbasephoenix --master local --deploy-mode client  file:///home/hadoop/sparkpocs_2.11-0.1.jar s3://satishbucket2019/input/us-500.csv
 */