package com.bigdata.spark.spark_databaseprocessing

import com.bigdata.spark.spark_databaseprocessing.joinproductionproperties._
import org.apache.spark.sql._

object importAllTables {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("importAllTables").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    import spark.sql

   //Not recommended as it emp is hardcorded
    val qry = "(select * from emp where sal>=3000) t"
    val df1 = spark.read.jdbc(ourl,qry,oprop)
    df1.show()

   /////////////////////String interpolation is used to get tables dynamically but this is also hardcoded so not recommended/////
    val tabs1 = Array("EMP","DEPT")
    tabs1.foreach{x =>
      val df2=spark.read.jdbc(ourl,s"$x",oprop)
      df2.show()
    }
    //////////////////////
    val alltabs = "(select table_name from all_tables where tablespace_name='USERS') t"
    //read all tables into a dataframe,then convert to rdd and then to list
    val tabs = spark.read.jdbc(ourl, alltabs, oprop).select($"TABLE_NAME").rdd.map(x => x(0)).collect.toList
    tabs.foreach{ x =>
      val df = spark.read.jdbc(ourl, s"$x", oprop)
      df.write.jdbc(murl, s"$x", mprop)
      df.show()

    }
    spark.stop()
  }
}
