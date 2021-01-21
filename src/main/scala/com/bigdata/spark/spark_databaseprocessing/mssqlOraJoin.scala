package com.bigdata.spark.spark_databaseprocessing

import org.apache.spark.sql._

object mssqlOraJoin {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mssqlOraJoin").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    // one way to get data from any database
    val qry = "(select * from abhidf where sal>2500) t"
    val msurl = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msdf = spark.read.format("jdbc").option("user", "msuername")
      .option("password", "mspassword")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", "dept").option("url", msurl).load()
    msdf.show() //sql server dataframe

    // second way
    val ourl = "jdbc:oracle:thin:@//oracledb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:1521/ORCL"
    import java.util.Properties
    val oprop = new Properties()
    oprop.setProperty("user", "ousername")
    oprop.setProperty("password", "opassword")
    oprop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    val odf = spark.read.jdbc(ourl, "EMP", oprop)
    odf.show() //oracle dataframe

    msdf.createOrReplaceTempView("ms")
    odf.createOrReplaceTempView("o")

    val join = spark.sql("select o.*,ms.loc,ms.dname from ms join o on o.deptno=ms.deptno")
    join.show()

    //Exception in thread "main" java.lang.ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver
    // if u get above error there is dependency problem. add mssql jdbc jar manually file> project structure> dependencies>+ > jars...+ add dependencies > ok
    val murl = "jdbc:mysql://mysqldb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:3306/mysqldb"
    // import java.util.Properties
    val mprop = new Properties()
    mprop.setProperty("user", "myusername")
    mprop.setProperty("password", "mypassword")
    mprop.setProperty("driver", "com.mysql.jdbc.Driver")
    join.write.mode(SaveMode.Overwrite).jdbc(murl, "mssqlorajointab1", mprop)
    spark.stop()
  }
}
