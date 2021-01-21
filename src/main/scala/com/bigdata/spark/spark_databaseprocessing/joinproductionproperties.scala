package com.bigdata.spark.spark_databaseprocessing

import org.apache.spark.sql._
// This program is like a package .No main method.
object joinproductionproperties {
    val murl ="jdbc:mysql://mysql.cq8av8lmgyn6.ap-south-1.rds.amazonaws.com/mysqlfirst"
     import java.util.Properties
    val mprop = new Properties()
    mprop.setProperty("user","myusername")
    mprop.setProperty("password","mypassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")

    val ourl ="jdbc:oracle:thin:@//oradb1.cq8av8lmgyn6.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")

    val msurl ="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val oemp="emp"

}
