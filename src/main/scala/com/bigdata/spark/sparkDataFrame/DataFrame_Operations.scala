package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._ //_ is all
import org.apache.spark.sql.functions._  //many functions are available in  this functions //not being used
//col is available within  import org.apache.spark.sql.functions._

//It is not possible recursively so 2 different  imports as  import org.apache.spark.sql._ will import only methods within .sql but not what is within .functions.
// so import org.apache.spark.sql.functions._ is written seperately

//$ or Col()

object DataFrame_Operations {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrame_Operations").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val data ="D:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header", "true").option("inferSchema","true").option("delimiter",",").load(data)
    df.createOrReplaceTempView("tab1") //Registers this DataFrame as a temporary table.

    /*
   df.first  //It returns the first row.
   df.take(8)  //If you want to display first n numer of rows
   df.head() //only 1 row
   df.head(5) //Returns the first n rows in the form of rows and Array formate. If you don't specify it's return only first row.
  df.collect()// Returns an array that contains all of Rows in this DataFrame.SIMILAR  tohead(all)
   df.count() //Returns the number of rows in the DataFrame.
   df.show()  //Displays the top 20 rows of DataFrame in a tabular form. You can increase/decrease Numer of rows in show(5) or show(100)
   df.cache() //Persist this DataFrame with the default storage level .cache() support only 2 memory storage level (MEMORY_ONLY AND MEMORY_AND_DISK
   df.persist() //Persist also similar to cache(), but it support many storage levels (MEMORY_AND_DISK_SER, DISK_ONLY and more)
   df.unpersist() //Mark the DataFrame as non-persistent, and remove all blocks for it from memory and disk. After cache() or persist() only apply this function.Automatically framework will delete if you are not using persist data frequently. If you want forcefully delete, use unpersist.Its to clean storage memory only
   df.schema //schema() and columns() also return columns, but not in tree format.
   df.printSchema //prints the schema in tree format
   df.columns //prints columns() also return columns, but not in tree format.
   df.where("age > 15")  //Filters rows using the given SQL expression
   df.filter(col("age") > 85).show() //Filter the data columns, based on condition. It return all rows.
   df.filter($"age"gt(85)).show() // df.filter($"age">85).show()
   df.drop("marital")   //Returns a new DataFrame with a column dropped.
   df.dropDuplicates()     //It's similar to distinct(). It returns a new DataFrame without any duplicate rows in the form of Array.
   df.orderBy(col("age"))  //Returns a new DataFrame sorted by the given expressions. This is an alias of the sort function.
  df.explain()           //Used to debugging purpose
  df.sort($"age", $"balance".desc).show()  //Sort the values based on expression. use asc or desc
  df.write.save("path")  //Interface for saving the content of the DataFrame out into external storage.

   //It creates a dataframe from RDD with columns renamed, but the data must be row/structured format
   //Must specify all columns to rename
   df.toDF("MyAge","MyJob","MyStatus","Study","bankBalance","housing","loan")

   //It used to rename the column name, you can rename any column.if you don't have head/schema data, to specify schema, it's highly recommended.
   df.withColumnRenamed("loan","bankLoan")

  //To get a specific columns use select function.
   df.select($"marital",$"age").show()
   df.select("marital","age").show()
   df.select(mean("age"), min("age"), max("age")).show()

   //Most frequently used with select(),to get desired columns use col(column)
   //col() return a column, but not return dataframe, but if you use with select, it's a dataframe. In If condition you can use it
   df.select(col("marital"),col("age")).show()

   //Returns a new DataFrame that contains only the unique rows from this DataFrame.
   //Please note it's applicable for a perticular column only.
   IF you apply for all dataframe, it's remove duplicate records.
  df.select(col("marital")).distinct.show()

 //It's most frequently used in complex json dataframes.
 //Let example you have 'Venu katragadda' split into two names to do it use
 explode(name)

//Groups the DataFrame using the specified columns, so we can run aggregation on them. It's wide operation
//// Compute the average for all numeric columns grouped by age.
//df.groupBy("age").avg()

//RightOuter join ====Join two datasets
import org.apache.spark.sql.functions._
df1.join(df2, df1("name") === df2("name"), "outer")

//return common values based on condition.
//// The following two are equivalent:
//df1.join(df2, df1("name") === df2("name"))
//df1.join(df2).where(df1("name") === df2("name"))


//Returns a new Dataset by adding a column or replacing the existing column that has the same name.
df.withColumn("full_name", concat($"fname", lit(" "), $"lname"))

//RDD operations that Dataframe support
//coalesce
//flatMap
//foreach
//map
//More
// UnionAll-Returns a new DataFrame containing union of rows in this frame and another frame. This is equivalent to UNION ALL in SQL


 */

    spark.stop()
  }
}
