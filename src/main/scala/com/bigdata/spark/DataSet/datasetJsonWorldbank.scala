package com.bigdata.spark.DataSet

import com.bigdata.spark.spark_databaseprocessing.joinproductionproperties.wbcc
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object datasetJsonWorldbank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datasetJsonWorldbank").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data= "file:///D:\\BigData\\Datasets\\world_bank.json"
    val df1=spark.read.format("json").option("inferSchema","true").load(data)
    //df.show()
    val df = df1.as[wbcc]
    df.printSchema()
    df.createOrReplaceTempView("jtab")
    //val res=spark.sql("select _id,city from jtab where state=\"NC\"")
    //val qry="select _id id,city, loc[0] longitude, loc[1] latitude,pop,state from jtab where state='NJ'"
    //val res=spark.sql(qry)
    //val res = df.withColumn("age",lit(25)).withColumn("city", when($"city"==="BLANDFORD","BLA").otherwise($"city")
    /*val res=df.withColumn("theme1name", $"theme1.name").withColumn("theme1percent",$"theme1.percent")
      .drop("theme1")
      .withColumn("theme_namecode",explode(col("theme_namecode")))
      .withColumn("majorsector_percent",explode(col("majorsector_percent")))
      .withColumn("mjsector_namecode",explode(col("mjsector_namecode")))
      .withColumn("mjtheme_namecode",explode(col("mjtheme_namecode")))
      .withColumn("projectdocs",explode(col("projectdocs")))
      .withColumn("sector",explode(col("sector")))
      .withColumn("sector_namecode",explode(col("sector_namecode")))
      .withColumn("idoid",$"_id.$$oid")


    //.withColumnRenamed("theme_namecode1.code", "tncode")
    //.withColumnRenamed("theme_namecode1","tnname")
    //res.printSchema()
    val res1= res.select($"",$"projectdocs.",$"theme_namecode.code".alias("theme_namecode_code"),$"theme_namecode.name".alias("theme_namecode_name")
      ,$"theme_namecode.*",$"sector1.Name".alias("sector1_name"),$"sector1.Percent".alias("sector1_Percent"),$"sector.Name".alias("sector_name"),$"sector2.Name".alias("sector2_name"),$"sector2.Percent".alias("sector2_Percent")
      ,$"sector3.Name".alias("sector3_name"),$"sector1.Percent".alias("sector3_Percent")
      ,$"sector4.Name".alias("sector4_name"),$"sector4.Percent".alias("sector4_Percent")
      ,$"sector_namecode.Name".alias("sector_namecode_name"),$"sector_namecode.code".alias("sector_namecode_code")
      ,$"project_abstract.cdata",$"mjtheme_namecode.Name".alias("mjtheme_namecode_name"),$"mjtheme_namecode.code".alias("mjtheme_namecode_code")
      ,$"mjsector_namecode.Name".alias("mjsector_namecode_name"),$"mjsector_namecode.code".alias("mjsector_namecode_code")
      ,$"majorsector_percent.Name".alias("majorsector_percent_name"),$"majorsector_percent.Percent".alias("majorsector_percent_Percent")
      ,explode($"mjtheme").alias("mjtheme_element")).drop("majorsector_percent","mjsector_namecode","mjtheme_namecode","sector","sector1","sector2","sector3"
      ,"sector4","sector_namecode","theme_namecode","projectdocs","project_abstract","mjtheme","_id")
    res1.printSchema()*/
    spark.stop()
  }
}
