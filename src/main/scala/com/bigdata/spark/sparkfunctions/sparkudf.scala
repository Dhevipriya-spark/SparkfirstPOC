package com.bigdata.spark.sparkfunctions

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//function is a object that can be stored in a variable
// method always belongs to a class which has a name and signature.method is a function which is a member of some object

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
/*
    //offer: String => String = <function1>
    //This is a anonymous fucntion
    val offer=(state:String)=>state match{
      case "OH"=> "20% off"
      case "NJ" | "CA"  =>"10 % off"
      case "NY" |"MI" | "IL" =>"30%off"
      case _  =>"no offer"
    }

    //spark don't know anything but support udf.. convert function to udf
    val uf = udf(offer)
    //uf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(StringType)))
    val res = df.withColumn("weekendoffer",uf($"state"))
    res.show(15,false)
*/
    //offer1: (state: String)String
    //This offer1 is a method.
    def offer1 (state:String)= state match {
      case "OH" => "20% off"
      case "NJ" | "CA" => "10% off"
      case "NY" | "MI" | "IL" => "30% off"
      case _ => "no offer"
    }

    //method to function
    //offer1 _ with underscore method gets converted to function
    // _ ensures that offer is a function

    spark.udf.register("off", udf(offer1 _)) // in spark sql must convert udf to a registered name
    val res1 = spark.sql("select *, off(state) monthoffer from tab")
    res1.show(15,false)
    res1.printSchema()
    spark.stop()
  }
}

/*
Simple method:
def m1(x: Int) = x + x
m1(2)  // 4

Simple function:
val f1 = (x: Int) => x + x
f1(2)  // 4

Both m1 and f1 are called the same way and will produce the same result but when
you look closer you will see that these are two different things.

f1  // Int => Int = <function1>
m1  // error: missing argument list for method m1...
Calling f1 without argument will give us the signature of anonymous function. Our anonymous
functions is actually an instance of Function1[Int, Int] that means a function with 1 parameter of type Int and return value of type Int.

f1.isInstanceOf[Function1[Int, Int]]  // Boolean = true
When calling m1 we will get an error. This is because m1 is not a value, it is a method, something that will produce a value when you call it by providing all required arguments.

Converting method into a function
Method can be converted into a proper function (often referred to as lifting) by calling method with underscore "_" after method name.

val f2 = m1 _  // Int => Int = <function1>
Alternatively you can supply a type and compiler will know what to do.

val f2: Int => Int = m1  // Int => Int = <function1>
Above method to function conversions will always create a new instance of Function1[Int, Int]
which is an important observation.

Let’s consider following example. We are creating a sequence of tuples with type (Int, Int => Int).
For each tuple we decided to convert our previously defined method into a function.
The following will create 10 instances of a function that does the same thing.

for (i <- 0 until 10) yield i -> m1 _
Better approach would be to pass the same instance of the function for each tuple and avoid
allocating memory for each instance as in previous approach.

for (i <- 0 until 10) yield i -> f1

When to use methods and when functions
use functions if you need to pass them around as parameters
use functions if you want to act on their instances, e.g. f1.compose(f2)(2)
use methods if you want to use default values for parameters e.g. def m1(age: Int = 2) = ...
use methods if you just need to compute and return
 */
