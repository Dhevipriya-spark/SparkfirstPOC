package com.bigdata.spark.DataSet

import com.bigdata.spark.spark_databaseprocessing.joinproductionproperties._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//case class uscc(first_name: String,last_name: String,company_name: String,address:String,city:String,county:String,state:String,zip:Int,phone1:String,phone2:String,email:String,web:String)
object datasetapi {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datasetapi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "D:\\BigData\\Datasets\\us-500.csv"
    val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)
    //df.show()
    val ds = df.as[uscc]
    //val ds1=df.as[(String,String,String.................)]
    // name: venu katragadda 9247159150
    //case class name(fname:String, lname:String, mobile:Long)
    ds.createOrReplaceTempView("tab")
    val res=spark.sql("select * from tab where state='NY'").as[uscc]
    //returning dataframe .so converting to ds using as[uscc]
    res.show()

    spark.stop()
  }
}