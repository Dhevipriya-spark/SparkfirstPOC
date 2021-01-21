package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparksqlregex {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparksqlregex").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data ="D:\\bigdata\\datasets\\10000Records.csv"
    val df = spark.read.format("csv").option("header", "true").option("inferSchema","true").option("delimiter",",").load(data)
    /*df: org.apache.spark.sql.DataFrame = [Emp ID,Name Prefix,First Name,Middle Initial,Last Name,Gender,E Mail,Father's Name,Mother's Name,Mother's
 Maiden Name,Date of Birth,Time of Birth,Age in Yrs.,Weight in Kgs.,Date of Joining,Quarter of Joining,Half of Joining,Year of Joining,Month of
 Joining,Month Name of Joining,Short Month,Day of Joining,DOW of Joining,Short DOW,Age in Company (Years),Salary,Last % Hike,SSN,Phone No. ,Pla
ce Name,County,City,State,Zip,Region,User Name,Password: string]

scala> df.columns
res16: Array[String] = Array(Emp ID, Name Prefix, First Name, Middle Initial, Last Name, Gender, E Mail, Father's Name, Mother's Name, Mother's
 Maiden Name, Date of Birth, Time of Birth, Age in Yrs., Weight in Kgs., Date of Joining, Quarter of Joining, Half of Joining, Year of Joining,
 Month of Joining, Month Name of Joining, Short Month, Day of Joining, DOW of Joining, Short DOW, Age in Company (Years), Salary, Last % Hike,
SSN, "Phone No. ", Place Name, County, City, State, Zip, Region, User Name, Password)

*/
    val reg = "[^a-zA-Z]"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    /*cols: Array[String] = Array(EmpID, NamePrefix, FirstName, MiddleInitial, LastName, Gender, EMail, FathersName, MothersName, MothersMaidenName,
DateofBirth, TimeofBirth, AgeinYrs, WeightinKgs, DateofJoining, QuarterofJoining, HalfofJoining, YearofJoining, MonthofJoining, MonthNameofJoin
ing, ShortMonth, DayofJoining, DOWofJoining, ShortDOW, AgeinCompanyYears, Salary, LastHike, SSN, PhoneNo, PlaceName, County, City, State, Zip,
Region, UserName, Password)
*/

    val ndf = df.toDF(cols:_*)
    /*
ndf: org.apache.spark.sql.DataFrame = [EmpID: int, NamePrefix: string ... 35 more fields]
   */



    ndf.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab")
    //res: org.apache.spark.sql.DataFrame = [EmpID: int, NamePrefix: string ... 35 more fields]
    res.show()
    spark.stop()
  }
}
