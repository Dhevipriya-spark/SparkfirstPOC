package com.bigdata.spark.sparkpractice

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object spark_definefunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("spark_definefunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    //def ==it is a method
       def saloff(sal:Int)={
         if(sal>1000 && sal<1500) sal*30/100
         else if(sal>=1500 && sal<2500) sal*40/100
         else if(sal>=2500 && sal<3000) sal*50/100
         else 0
       }

    val df=spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno","empname","job","mgr","hiredate","sal","comm","deptno")

    df.show()
    df.createOrReplaceTempView("tab")
    //_ is used to convert a method to a function
    val uoffer=udf(saloff _)
    val res=df.withColumn("todayoffer",uoffer($"sal"))
    res.show()

    spark.udf.register("myoffer",uoffer)
    val res1=spark.sql("select *,myoffer(sal) bonus from tab")
    res1.show()

   // val sal=2000
    val saloffer=(sal:Int)=>sal match {
      case sal if(sal > 1000 && sal < 1500)=> sal*30/100
      case sal if(sal >= 1500 && sal < 2500)=> sal*40/100
      case sal if(sal >= 2500 && sal < 3000)=> sal*50/100
      case _ => 0
    }
    val uof=udf(saloffer)
    val res3=df.withColumn("todayoffer",uof($"sal"))
    res3.show()
    spark.udf.register("myoffer",uof)
    val res4=spark.sql("select *,myoffer(sal) bonus from tab")
    res4.show()

    spark.stop()

  }
}
