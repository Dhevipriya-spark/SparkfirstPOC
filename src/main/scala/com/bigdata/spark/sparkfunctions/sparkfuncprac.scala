package com.bigdata.spark.sparkfunctions
//Intellij Invalidate Cache and Restart
//https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkfuncprac {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfuncprac").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val data="D:\\BigData\\Datasets\\bank-full.csv"

    val df=spark.read.format("csv").option("header","true").option("inferschema","true").option("delimiter",";").load(data)
    df.createOrReplaceTempView("tab")
    //df.printSchema()

    //val res=spark.sql("select * from  tab where balance>70000 and  marital='married'")

    // val res=df.where($"balance">70000 && $"marital"==="married")
    // val res=df.where($"balance">70000 and $"marital"==="married")

    //val res= spark.sql("select *,concat(job,'-',marital,'-',balance) fullname,concat_ws('-',job,marital,balance) fullname_ws from tab")

    //val res=df.withColumn("fullname",concat_ws("-",$"job",$"marital",$"balance"))
    // .withColumn("fullname1",concat($"job",lit("-"),$"marital",lit("-"),$"balance"))

    /*val res=df.withColumn("job",regexp_replace($"job","-",""))
     .withColumn("marital",regexp_replace($"marital","single","Bachelor"))
     .withColumn("marital",regexp_replace($"marital","Divorced","seperated"))
     .withColumn("marital",when($"marital"==="Married","couple")
       .when($"marital"==="seperated","divorced")
      .otherwise("single"))*/

    //val res=df.groupBy($"marital").count()
    //val res=df.groupBy($"marital").count().alias("cunt")  //Alias does not work
    //val res=df.groupBy($"marital").max() //max of all columns
    val res=df.groupBy($"marital").max("age") //maximum age
    //val res=df.groupBy($"marital").count().orderBy($"count".desc)

    //agg  ====is used when we want a alias
    //val res=df.groupBy($"marital").agg(count("*"))
    //val res=df.groupBy($"marital").agg(count("*").alias("cnt"))
    //val res=df.groupBy($"marital").agg(count($"marital").as("count"))
    //val res=df.groupBy($"marital").agg(count("*").alias("cnt")).orderBy($"cnt".asc)
    res.show( false)

    spark.stop()
  }
}
