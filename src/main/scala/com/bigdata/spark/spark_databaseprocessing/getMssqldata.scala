package com.bigdata.spark.spark_databaseprocessing

import org.apache.spark.sql._

//https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
//spark.sql has DataFrameReader.DataFrameReader by default supports csv,jdbc,table(hive),json,options,orc,parquet,text,textFile
//but does not support avro .so in such cases use spark packages.sparks supports jdbc(oracle,mysql,mssql....)
object getMssqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getMssqldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val qry = "(select * from abhidf where sal>2500) t"
    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"

    val df = spark.read.format("jdbc").option("user", "msuername").option("password", "mspassword").
      option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", "abhidf").
      option("url", url).load()
    df.show()
    //if you get is ClassNotfoundException.Add mssql driver jar to File==>projectstructure ==>project setting ===>module==> + Add jars

    // 't' in qry is passed to qry variable in dbtable option
    val df1 = spark.read.format("jdbc").option("user", "msuername").option("password", "mspassword")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", qry).option("url", url).load()
    df1.show()

    //option("query","select* from abhidf")

    //recommended for scala/java
    import java.util.Properties
    //second way to get data from db
    val msprop = new Properties()
    msprop.setProperty("user", "msuername")
    msprop.setProperty("password", "mspassword")
    msprop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val msdf = spark.read.jdbc(url, qry, msprop)
    //val msdf = spark.read.jdbc(url,"abhidf",msprop)
    msdf.show()

    spark.stop()
  }
}
