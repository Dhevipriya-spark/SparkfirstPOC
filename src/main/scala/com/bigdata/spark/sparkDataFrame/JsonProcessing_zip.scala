package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/* Spark DataframeReader by default supports csv..json..
public Dataset<Row> json(scala.collection.Seq<String> paths)
Loads JSON files and returns the results as a DataFrame.
JSON Lines (newline-delimited JSON) is supported by default. For JSON (one record per file), set the multiLine option to true.

This function goes through the input once to determine the input schema. If you know the schema in advance, use the version that specifies the schema to avoid the extra scan.

https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
*/

object JsonProcessing_zip {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JsoonProcessing").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val data = "file:///D:\\bigdata\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
    df.show()
    df.createOrReplaceTempView("tab")
    df.printSchema()
    /*root
 |-- _id: string (nullable = true)
 |-- city: string (nullable = true)
 |-- loc: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- pop: long (nullable = true)
 |-- state: string (nullable = true)
     */
    val qry = "select _id id, city , loc[0] long, loc[1] Lat, pop, state from tab "
    val res = spark.sql(qry)
    res.printSchema()
    /* root
|-- id: string (nullable = true)
|-- city: string (nullable = true)
|-- long: double (nullable = true)
|-- Lat: double (nullable = true)
|-- pop: long (nullable = true)
|-- state: string (nullable = true)
*/
   val qry1= "select _id id,city,loc[0] Long,loc[1] Lat,pop,state from tab where state ='NJ'"
    val res1=spark.sql(qry1)
   // res1.show()

    val qry2 = "select state, city from tab group by state, city order by city asc "
    val res2 = spark.sql(qry2)
    //res2.show()

    //abs= absolute value ..likewise lot of functions are available
    val qry3= "select _id id,city,abs(loc[0]) Long,loc[1] Lat,pop,state from tab where state ='NJ'"
    val res3=spark.sql(qry3)
    res3.show()

    /*
    //withColumn used to create a new column if the column doesnot exists,if the column exists it update column
   -withColumn-Returns a new Dataset by adding a column or replacing the existing column that has the same name.
             -column's expression must only refer to attributes supplied by this Dataset. It is an error to add a column that refers to some other Dataset.
   -lit -Creates a Column of literal value.(Dummy value)
   withColumn applicable only on data  and not on schema

//when
  public static Column when(Column condition,Object value)
    Evaluates a list of conditions and returns one of multiple possible result expressions. If otherwise is not defined at the end, null is returned for unmatched conditions.
    people.select(when(people("gender") === "male", 0)
     .when(people("gender") === "female", 1)
     .otherwise(2))

     Lot of functions exist like from_json,get_json_object
    */
    //Process using DSL
    val res5=df.withColumn("age",lit("18")).withColumn("city",when($"city"==="BLANDFORD","BLA").otherwise($"city"))
    .withColumn("state",regexp_replace($"state","MA","MAHARASHTRA"))
    res5.show()
    /*
+-----+---------------+--------------------+-----+-----+---+
|  _id|           city|                 loc|  pop|state|age|
+-----+---------------+--------------------+-----+-----+---+
|01001|         AGAWAM|[-72.622739, 42.0...|15338|   MA| 18|
|01002|        CUSHMAN|[-72.51565, 42.37...|36963|   MA| 18|
|01005|          BARRE|[-72.108354, 42.4...| 4546|   MA| 18|
|01007|    BELCHERTOWN|[-72.410953, 42.2...|10579|   MA| 18|
|01008|            BLA|[-72.936114, 42.1...| 1240|   MA| 18|
|01010|      BRIMFIELD|[-72.188455, 42.1...| 3706|   MA| 18|
|01011|        CHESTER|[-72.988761, 42.2...| 1688|   MA| 18|
     */

    //val res4=df.select($"_id".alias("id"),$"city",$"loc[0]".alias("Long"),$"loc[1]".alias("Lat"),$"pop",$"state" )

    spark.stop()
  }
}
