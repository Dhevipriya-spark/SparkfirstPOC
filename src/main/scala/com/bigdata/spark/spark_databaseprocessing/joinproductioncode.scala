package com.bigdata.spark.spark_databaseprocessing

import org.apache.spark.sql._
import com.bigdata.spark.spark_databaseprocessing.joinproductionproperties._
object joinproductioncode {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("joinsproductionStyle").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val msdf = spark.read.jdbc(msurl,"dept",msprop)
    msdf.show()

    val odf = spark.read.jdbc(ourl,"EMP",oprop)
    val odf1 = spark.read.jdbc(ourl,oemp,oprop)
    odf.show()

    msdf.createOrReplaceTempView("ms")
    odf.createOrReplaceTempView("o")
    val join = spark.sql("select o.*,ms.loc,ms.dname from ms join o on o.deptno=ms.deptno")
    join.show()

    join.write.mode(SaveMode.Overwrite).jdbc(murl,"mssqlorajointab",mprop)

    spark.stop()
  }
}
