package com.bigdata.spark.sparkfunctions

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkudf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkudf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data = "D:\\BigData\\Datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",").load(data)
    //df.show()
    df.createOrReplaceTempView("tab")

    /*//offer: String => String = <function1>
    val offer=(state:String)=>state match{
      case "OH"=> "20% off"
      case "NJ" | "CA"  =>"10 % off"
      case "NY" |"MI" | "IL" =>"30%off"
      case _  =>"no offer"
    }*/

    //offer: (state: String)String
    def offer (state:String)= state match {
      case "OH" => "20% off"
      case "NJ" | "CA" => "10% off"
      case "NY" | "MI" | "IL" => "30% off"
      case _ => "no offer"
    }
    //method to function
        //offer _ with underscore method gets converted to function
        // _ ensures that offer is a function

    val uf = udf(offer _) //spark don't know anything but support udf.. convert function to udf
    //uf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(StringType)))
    val res = df.withColumn("weekendoffer",uf($"state"))
        //res.show(15,false)
/*
    spark.udf.register("off", udf(offer _)) // in spark sql must convert udf to a registered name
    val res = spark.sql("select *, off(state) monthoffer from tab")*/
    res.show(15,false)
    res.printSchema()
    spark.stop()
  }
}
