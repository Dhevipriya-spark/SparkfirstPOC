package com.bigdata.spark.sparkfunctions

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// if def is there class inside  then it is a method
// if def is there	object inside then it is a function
object sparkDateFunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.sql.session.timeZone","IST").appName("sparkDateFunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val df = spark.createDataFrame(Seq(
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
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

   df.createOrReplaceTempView("tab")
   //df.printSchema()
   //val res=spark.sql("select * from tab where sal>2000")
   //res.show()

  //Convert String to Date datatype use to_date()
    //format should be same as inputdata .As its 23-May-87  use dd-MMM-yy
    //A date, timestamp or string. If a string, the data must be in a format that can be
    // cast to a date,such as yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.SSSS
    val res1=df.withColumn("hiredate",to_date($"hiredate","dd-MMM-yy"))
     //res1.show()
    // output date gets converted to 1981-06-09
    val res2=spark.sql("select empno,ename,job,mgr,to_date(hiredate,'dd-MMM-yy') hiredate,sal,comm,deptno from tab")
    //res2.show()

    //month()-Extracts the month as an integer from the given timestamp/time/string
    //Alias works for select and not for where
    val res3=res1.where((month($"hiredate")>=6).as("hire_2ndquarter"))
    //res3.show()
    val res31=res1.select((month($"hiredate")>=6).as("hire_2ndquarter"))
    //res31.show()

   //current_date()
   val res4=res1.where(month($"hiredate")>=6).withColumn("today",current_date())
    //res4.show()

    //dateDiff-Difference between hiredate and today date(-Employee Experience)
    //How many days difference between 2 dates
    val res5=res1.where(month($"hiredate")>=6).withColumn("today",current_date())
      .withColumn("EmpExp_days",datediff($"today",$"hiredate")) //days of experience
      .withColumn("EmpExp_years",round(datediff($"today",$"hiredate")/365,2)) //years of exp rounded 2 digits
       .withColumn("monthsdiff",months_between($"today",$"hiredate")) //month of experience (Total no of days/31)
      .withColumn("months_diff",round(months_between($"today",$"hiredate"),0).cast("Integer")) ////month of experience (Total no of days/31) as an integer
     //  res5.show()

    //Timezone -use config("spark.sql.session.timeZone","IST")
    val ti=spark.conf.get("spark.sql.session.timeZone")
   // println("The timezone with which data is processed :"+ti)

 //Add or subtract months /dates
 // add_months,date_add,date_sub
    val res6=res1.withColumn("today",current_date())
      .withColumn("fwd_date",add_months($"today",100)) //After 100 months what is the  date
      .withColumn("old_date",add_months($"today",-100))//Before 100 months what is the date
      .withColumn("addates",date_add($"hiredate",9))//what is date after 9 days
      .withColumn("subdates",date_add($"hiredate",-9))//what is date before 9 days
      .withColumn("sub_dates",date_sub($"hiredate",9))//what is date before 9 days
    //res6.show()

    //date_trunc==Reset to 1 st of year/month/day and remaining is truncated
    //you can truncate month,year,day,hour,min,sec
    val res7=res1.withColumn("today",current_date())
      .withColumn("year_trunc",date_trunc("year",$"hiredate"))//Reset to 1st day of the year
      .withColumn("mont_trunc",date_trunc("month",$"hiredate"))//Reset to 1st of month for each month
  //  res7.show()

  //To calculate salary when the person's last working day is 23rd of this month.To give 23 days salary
  val res8=res1.withColumn("today",current_date())
    .withColumn("lday",date_trunc("month",$"today"))
    .withColumn("no_days_sal",datediff($"today",$"lday"))
    //res8.show()


    // what is next friday from today
    //last_day===Returns the last day of the month which the given date belongs to. For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the month in July 2015.
    val res9=res1.withColumn("today",current_date())
      .withColumn("nxtFri",next_day($"today","Friday"))
   //res9.show()

    //To find out every last friday(Salary day) of the month
    // Returns the first date which is later than the value of the `date` column that is on the specified day of the week.
    //   For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first Sunday after 2015-07-27.
    val res10=res1.withColumn("today",current_date())
      .withColumn("sal_date",next_day(date_add(last_day(current_date()),-7),"Friday"))
   // res10.show()

 //First salary day .use hire_date
    val res11=res1.withColumn("today",current_date())
      .withColumn("First_Sal_Date",next_day(date_add(last_day($"hiredate"),-7),"Friday")) //date_sub
    //res11.show()

    //Day of year/weekofyear/dayofmonth
    //dayofyear -how many days completed from 1st jan
    val res12=res1.withColumn("today",current_date())
      .withColumn("dayofyear",dayofyear($"today"))//dayofmonth(),dayofweek()
         .withColumn("weekofyear",weekofyear($"today"))
      .withColumn("dayofmonth",dayofmonth($"today"))
  //  res12.show()

//How many days completed till the hiredate
    val res13=res1.withColumn("today",current_date())
      .withColumn("joining_dayofyear",dayofyear($"hiredate"))//dayofmonth(),dayofweek( sun-1,mon-2,wed-3)
      .withColumn("Joining_weekofyear",weekofyear($"hiredate"))
      .withColumn("Joining_dayofmonth",dayofmonth($"hiredate"))
   // res13.show()

    //How many days in current month ////Assignment================????????????????????
    //      // get last day of month and fetch only day at that time use day of month.
    val res14=res1.withColumn("daysinmonth",dayofmonth(last_day(current_date())))
   //res14.show

    //date_format
    //u- Mon-1,Tue-2,Wed-3,Thursday-4.....sunday-7
    //u is unregistered ...E it will Fri,Sat,Sun
    //Dont recommend this function to convert forom one date format to a another date format .use to_date.
    //DateFormat is used when u need Friday,1
    val res15=res1.withColumn("today",current_date())
      .withColumn("date_format_number",date_format($"today","u"))
      .withColumn("date_format1_Day",date_format($"today","EEEE"))
    //res15.show

    //Timestamp
    //unix_timestamp:Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the default timezone and the default locale.
   //how many seconds completed from 1st jan 1970
    val res16=res1.withColumn("unixtimestamp",unix_timestamp(current_timestamp()))
                  .withColumn("currenttimestamp",current_timestamp())
      .withColumn("Convert_unixts_to_normalts",from_unixtime($"unixtimestamp"))
      .withColumn("pst_ist",from_utc_timestamp(from_unixtime($"unixtimestamp"),"IST"))//if london time 9:38am whats indian time(5:30 hrs ahead)


    res16.show(false)


    spark.stop()
  }
}

/*from_utc_timestamp
    Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield
   * '2017-07-14 03:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone that the input should be adjusted to, such as
   *           `Europe/London`, `PST` or `GMT+5`
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
     */
