package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ExtractEmailDomain {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ExtractEmailDomain").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    //create an example dataframe
    val df = Seq((1, "ii@koko.com"),
      (2, "lol@fsa.org"),
      (3, "kokojambo@mon.eu"))
      .toDF("id", "email")

    //original dataframe
    df.show(false)
    //output
    //    +---+----------------+
    //    |id |email           |
    //    +---+----------------+
    //    |1  |ii@koko.com     |
    //    |2  |lol@fsa.org     |
    //    |3  |kokojambo@mon.eu|
    //    +---+----------------+

    //using regex get the domain name
    df.withColumn("domain",
      regexp_extract($"email", "(?<=@)[^.]+(?=\\.)", 0))
      .show(false)

    //output
    //    +---+----------------+------+
    //    |id |email           |domain|
    //    +---+----------------+------+
    //    |1  |ii@koko.com     |koko  |
    //    |2  |lol@fsa.org     |fsa   |
    //    |3  |kokojambo@mon.eu|mon   |

    spark.stop()
  }
}
