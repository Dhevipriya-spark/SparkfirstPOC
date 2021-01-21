package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bigdata.spark.spark_databaseprocessing.joinproductionproperties._


/* json ...used in mobiles, cloud ,satellites, social media, most of servers generate json format. light weight format and support any datatypes.
JSON: JavaScript Object Notation.
JSON is a lightweight data-interchange format
JSON is a syntax for storing and exchanging data.
JSON is text, written with JavaScript object notation.
When exchanging data between a browser and a server, the data can only be text.
JSON is text, and we can convert any JavaScript object into JSON, and send JSON to the server.
We can also convert any JSON received from the server into JavaScript objects.
This way we can work with the data as JavaScript objects, with no complicated parsing and translations.

JSON is web friendly,server friendly,lightweight.servers also generating json data.

Why use JSON?
Since the JSON format is text only, it can easily be sent to and from a server, and used as a data format by any programming language.
JavaScript has a built in function to convert a string, written in JSON format, into native JavaScript objects:
JSON.parse()
So, if you receive data from a server, in JSON format, you can use it like any other JavaScript object.
============
oracle does not support array ,struct etc so we need to change to primitive datatype


 */
object ComplexJsonProcessing_worldbank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonProcessing").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data="file:///D:\\bigdata\\datasets\\world_bank.json"
    val df=spark.read.format("json").load(data)
    df.printSchema()
    //data cleaning process
  // *1*if you have struct column-if you have struct use parent column.child column eg:theme1  --theme1.name,theme1.percent
    //val res=df.withColumn("theme1name",$"theme1.Name").withColumn("theme1percent",$"theme1.Percent").drop($"theme1")
    //res.show(5,false)
    //res.printSchema()

    /*
     |-- theme1: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 =================
     |-- theme1name: string (nullable = true)
    |-- theme1percent: long (nullable = true)
     */
    //*2*struct within array -eg:theme_namecode
    // explode remove array from array(struct type) --updating the original theme_namecode
   // val res1=df.withColumn("theme_namecode",explode($"theme_namecode"))
    //res1.show(5,false)
   // res1.printSchema()
    /*
     |-- theme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 ========================
     |-- theme_namecode: struct (nullable = true)
 |    |-- code: string (nullable = true)
 |    |-- name: string (nullable = true)

     */
 // for All columns array is removed
    //explode===>Creates a new row for each element in the given array or map column.
    val res = df.withColumn("theme1name",$"theme1.Name").withColumn("theme1percent",$"theme1.Percent")
      .drop($"theme1")
      .withColumn("theme_namecode", explode(col("theme_namecode")))
      .withColumn("sector_namecode", explode(col("sector_namecode")))
      .withColumn("sector", explode(col("sector")))
      .withColumn("mjsector_namecode", explode(col("mjsector_namecode")))
      .withColumn("majorsector_percent", explode(col("majorsector_percent")))
      .withColumn("projectdocs", explode(col("projectdocs")))
      .withColumn("mjtheme_namecode", explode(col("mjtheme_namecode")))
      .withColumn("idoid",$"_id.$$oid") //As $ is present in a column $oid: string (nullable = true)

    res.printSchema()
//removing struct =============>
    //*=>As columns in  struct projectdocs ,project_abstract are unique select($"*",$"projectdocs.*",$"project_abstract.*") is sufficient
    //alias=>As columns in struct sector_namecode,theme_namecode sector 1,2,3,4,mjtheme_namecode,mjsector_namecodeare repetitive use alias,withColumnRenamed & withColumn can be used

    val res1=res.select($"*",$"projectdocs.*",$"project_abstract.*",$"theme_namecode.name".alias("theme_namecodename"),$"theme_namecode.code".alias("theme_namecodecode"),$"sector_namecode.code".alias("sector_namecodecode"),$"sector_namecode.name".alias("sector_namecodename"),
    $"sector4.Name".alias("sector4name"),$"sector4.Percent".alias("sector4percent"),
    $"sector1.Name".alias("sector1name"),$"sector1.Percent".alias("sector1percent"),
    $"sector2.Name".alias("sector2name"),$"sector2.Percent".alias("sector2percent"),
      $"sector3.Name".alias("sector3name"),$"sector3.Percent".alias("sector3percent"),
    $"sector.Name".alias("sectorname"),$"project_abstract.cdata".alias("project_abstractcdata"), $"majorsector_percent.Name".alias("majorsector_percentname"),$"majorsector_percent.Percent".alias("majorsector_percentpercent"),
      $"mjtheme_namecode.code".alias("mjtheme_namecodecode"),$"mjtheme_namecode.name".alias("mjtheme_namecodename"),
    $"mjsector_namecode.code".alias("mjsector_namecodecode"),$"mjsector_namecode.name".alias("mjsector_namecodename"),
      explode($"mjtheme").alias("mjthemedata"))
      .drop("mjtheme","_id","projectdocs","theme_namecode","project_abstract","mjsector_namecode","mjtheme_namecode","sector_namecode","sector4","sector3","sector2","sector1","sector","majorsector_percent")

    res1.show(5,false)
    res1.printSchema()
    //Data Cleaning processing complete
     //export data to mssql
    res1.write.jdbc(msurl,"worldbankDP",msprop)

    val res2 = spark.sql("select _id.`$oid` idoid, approvalfy, board_approval_month, boardapprovaldate, borrower, closingdate, country_namecode, countrycode, countryname, countryshortname, docty, envassesmentcategorycode, grantamt, ibrdcommamt, id, idacommamt, impagency, lendinginstr, lendinginstrtype, lendprojectcost, mp.Name mjpName, mp.Percent mjppercent, mn.code mncode, mn.name mnname, mjtheme[0] mjfirst, mjtheme[1] mjsecond, mjn.code mjncode, mjn.name mjnname, mjthemecode, prodline, prodlinetext, productlinetype, project_abstract.cdata projabscdata, project_name, pd.DocDate documentDate, pd.DocType documenttype, pd.DocTypeDesc documentinfo, pd.DocURL documenturl, pd.EntityID docentityid, projectfinancialtype, projectstatusdisplay, regionname, sec.Name sectorname, sector1.Name sec1name,sector1.Percent sec1percent, sector2.Name sec2name,sector2.Percent sec2percent, sector3.Name sec3name,sector3.Percent sec3percent, sector4.Name sec4name,sector4.Percent sec4percent, snc.code secnamecode, snc.name secnamename, sectorcode, source, status, supplementprojectflg, theme1.Name themename, theme1.Percent theme1perc, them.code themenamecode, them.name themenameonly, themecode, totalamt, totalcommamt, url from tab lateral view explode(theme_namecode) a as them lateral view explode(sector_namecode) a as snc lateral view explode(sector) t as sec lateral view explode(projectdocs) t as pd lateral view explode(majorsector_percent) t as mp lateral view explode(mjsector_namecode) t as mn lateral view explode(mjtheme_namecode) a")
    res2.show(5,false)
    res2.printSchema()

    spark.stop()
  }
}

/*
root
 |-- _id: struct (nullable = true)
 |    |-- $oid: string (nullable = true)
 |-- approvalfy: string (nullable = true)
 |-- board_approval_month: string (nullable = true)
 |-- boardapprovaldate: string (nullable = true)
 |-- borrower: string (nullable = true)
 |-- closingdate: string (nullable = true)
 |-- country_namecode: string (nullable = true)
 |-- countrycode: string (nullable = true)
 |-- countryname: string (nullable = true)
 |-- countryshortname: string (nullable = true)
 |-- docty: string (nullable = true)
 |-- envassesmentcategorycode: string (nullable = true)
 |-- grantamt: long (nullable = true)
 |-- ibrdcommamt: long (nullable = true)
 |-- id: string (nullable = true)
 |-- idacommamt: long (nullable = true)
 |-- impagency: string (nullable = true)
 |-- lendinginstr: string (nullable = true)
 |-- lendinginstrtype: string (nullable = true)
 |-- lendprojectcost: long (nullable = true)
 |-- majorsector_percent: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Name: string (nullable = true)
 |    |    |-- Percent: long (nullable = true)
 |-- mjsector_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- mjtheme: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- mjtheme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- mjthemecode: string (nullable = true)
 |-- prodline: string (nullable = true)
 |-- prodlinetext: string (nullable = true)
 |-- productlinetype: string (nullable = true)
 |-- project_abstract: struct (nullable = true)
 |    |-- cdata: string (nullable = true)
 |-- project_name: string (nullable = true)
 |-- projectdocs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- DocDate: string (nullable = true)
 |    |    |-- DocType: string (nullable = true)
 |    |    |-- DocTypeDesc: string (nullable = true)
 |    |    |-- DocURL: string (nullable = true)
 |    |    |-- EntityID: string (nullable = true)
 |-- projectfinancialtype: string (nullable = true)
 |-- projectstatusdisplay: string (nullable = true)
 |-- regionname: string (nullable = true)
 |-- sector: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Name: string (nullable = true)
 |-- sector1: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector2: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector3: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector4: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- sector_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- sectorcode: string (nullable = true)
 |-- source: string (nullable = true)
 |-- status: string (nullable = true)
 |-- supplementprojectflg: string (nullable = true)
 |-- theme1: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 |-- theme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- themecode: string (nullable = true)
 |-- totalamt: long (nullable = true)
 |-- totalcommamt: long (nullable = true)
 |-- url: string (nullable = true)
==============================================

 */