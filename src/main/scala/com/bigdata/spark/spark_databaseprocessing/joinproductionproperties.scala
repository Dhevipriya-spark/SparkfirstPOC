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


    case class uscc(first_name: String,last_name: String,company_name: String,address:String,city:String,county:String,state:String,zip:Int,phone1:String,phone2:String,email:String,web:String)
    case class _id(
                    `$oid`: String
                  )
    case class Majorsector_percent(
                                    Name: String,
                                    Percent: Double
                                  )
    case class Mjsector_namecode(
                                  name: String,
                                  code: String
                                )
    case class Project_abstract(
                                 cdata: String
                               )
    case class Projectdocs(
                            DocTypeDesc: String,
                            DocType: String,
                            EntityID: String,
                            DocURL: String,
                            DocDate: String
                          )
    case class Sector(
                       Name: String
                     )
    case class wbcc(
                     _id: _id,
                     approvalfy: String,
                     board_approval_month: String,
                     boardapprovaldate: String,
                     borrower: String,
                     closingdate: String,
                     country_namecode: String,
                     countrycode: String,
                     countryname: String,
                     countryshortname: String,
                     docty: String,
                     envassesmentcategorycode: String,
                     grantamt: Double,
                     ibrdcommamt: Double,
                     id: String,
                     idacommamt: Double,
                     impagency: String,
                     lendinginstr: String,
                     lendinginstrtype: String,
                     lendprojectcost: Double,
                     majorsector_percent: List[Majorsector_percent],
                     mjsector_namecode: List[Mjsector_namecode],
                     mjtheme: List[String],
                     mjtheme_namecode: List[Mjsector_namecode],
                     mjthemecode: String,
                     prodline: String,
                     prodlinetext: String,
                     productlinetype: String,
                     project_abstract: Project_abstract,
                     project_name: String,
                     projectdocs: List[Projectdocs],
                     projectfinancialtype: String,
                     projectstatusdisplay: String,
                     regionname: String,
                     sector: List[Sector],
                     sector1: Majorsector_percent,
                     sector2: Majorsector_percent,
                     sector3: Majorsector_percent,
                     sector4: Majorsector_percent,
                     sector_namecode: List[Mjsector_namecode],
                     sectorcode: String,
                     source: String,
                     status: String,
                     supplementprojectflg: String,
                     theme1: Majorsector_percent,
                     theme_namecode: List[Mjsector_namecode],
                     themecode: String,
                     totalamt: Double,
                     totalcommamt: Double,
                     url: String
                   )
}

