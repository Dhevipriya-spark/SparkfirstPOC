package com.bigdata.spark.sparkfunctions

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkfuncprac1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfuncprac1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data="D:\\BigData\\Datasets\\us-500.csv"

    val df=spark.read.format("csv").option("header","true").option("inferschema","true").option("delimiter",",").load(data)
    df.createOrReplaceTempView("tab")
    df.printSchema()

    //val res=df.groupBy($"state").count().orderBy($"count".desc)
   // val res=df.groupBy($"state").agg(count($"zip").as("zip")).orderBy("zip")

   // val res= spark.sql("select state,city,collect_list(first_name) allnames from tab  group by state,city")

   /*
    +-----+------------+--------------------------------------------------------------------+
|state|city        |allnames                                                            |
+-----+------------+--------------------------------------------------------------------+
|IN   |Indianapolis|[Carey, Raymon, Malinda, Reita]                                     |
|PA   |Jenkintown  |[Amber]                                                             |
|NC   |Fayetteville|[Lonna]                                                             |
|FL   |Orlando     |[Avery, Martina, Denise, Sharika, Chauncey]                         |
|FL   |Homestead   |[Pamella]                                                           |
|SC   |Spartanburg |[Eun]                                                               |
    */

    val res=df.groupBy($"state",$"city").agg(collect_list($"first_name").alias("names"))
   /*
   https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html
    */

    res.show(false )



    spark.stop()
  }
}
