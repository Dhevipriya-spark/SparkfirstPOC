package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._ //_ is all
import org.apache.spark.sql.functions._  //many functions are available in  this functions //not being used
//col is available within  import org.apache.spark.sql.functions._

//It is not possible recursively so 2 different  imports as  import org.apache.spark.sql._ will import only methods within .sql but not what is within .functions.
// so import org.apache.spark.sql.functions._ is written seperately
 // if we are not sure of the imports then highlight the red word and then give Alt +Enter will automatically import all the necessary packages
//$ or Col() means referencing

object US500EmailDomain {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("US500EmailDomain").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data ="D:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header", "true").option("inferSchema","true").option("delimiter",",").load(data)
    df.createOrReplaceTempView("tab1") //Registers this DataFrame as a temporary table.
    df.printSchema
    val res=spark.sql("select (SUBSTRING_INDEX(SUBSTR(email,INSTR(email, '@') + 1),'.',1)) as domain_name,count(*) cnt from tab1 group by domain_name order by cnt desc ")
   /* //substring_index(str: Column, delim: String, count: Int): Column
    //Returns the substring from string str before count occurrences of the delimiter delim.
      -----If count is positive, everything the left of the final delimiter (counting from left) is returned
      -----If count is negative, every to the right of the final delimiter (counting from the right) is returned
    //substring(str: Column, pos: Int, len: Int): Column	Substring starts at `pos` and is of length `len` when str is String type
    //instr(str: Column, substring: String): Column	Locate the position of the first occurrence of substr column in the given string.
  */

    //res.show()

    //show all the records
    val cnt = df.count().toInt //show the count of all the records
    df.show(cnt,false)//Dont truncate ..show the whole column value

    //show n number of rows
    df.show(8,false) //Dont truncate
    //if the string is more than 20 characters ,it will be truncated.

    /*
    //  $("") and col("") both are used to query specific a column
    ==== means u are using column
    == means u are using string (true or false)
    = means update the data

    //python using ==

     */
    val res1=df.where(col("state")==="NY") //$ or col lets spark know its a column.
    val res2=df.where($"state"==="OH")
    //As per spark/scala anything within double quotes is string
    val res3=df.groupBy($"state").count().orderBy($"count".desc)
    val res4 =df.select($"zip",$"state").filter($"zip">90000)
   //df.select($"zip",$"state").filter($"zip>90000") //Not recommended
    df.select($"zip",$"state").filter($"zip">90000 && $"state"==="AK").show()
    df.select($"zip",$"state").where($"zip">90000 && $"state"==="AK").show()
    res1.show()
    //no  truncation of values of column
    res1.show(5,false)
    //res2.show()
    //res3.show()
    res4.show()
    spark.stop()
  }
}
