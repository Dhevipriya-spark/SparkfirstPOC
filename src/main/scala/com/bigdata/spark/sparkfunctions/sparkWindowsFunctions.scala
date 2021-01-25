package com.bigdata.spark.sparkfunctions

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/* https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-window-functions-7b4e39ad3c86
A window function calculates a return value for every input row of a table based on a group of rows, called the Frame. Every input row can have a unique frame associated with it. This characteristic of window functions makes them more powerful than other functions and allows users to express various data processing tasks that are hard (if not impossible) to be expressed without window functions in a concise way.
to define a window specification, users can use the following syntax in SQL.

OVER (PARTITION BY ... ORDER BY ... frame_type BETWEEN start AND end)

Here, frame_type can be either ROWS (for ROW frame) or RANGE (for RANGE frame); start can be any of UNBOUNDED PRECEDING, CURRENT ROW, <value> PRECEDING, and <value> FOLLOWING; and end can be any of UNBOUNDED FOLLOWING, CURRENT ROW, <value> PRECEDING, and <value> FOLLOWING.

from pyspark.sql.window import Window
# Defines partitioning specification and ordering specification.
windowSpec = Window.partitionBy(...).orderBy(...)
# Defines a Window Specification with a ROW frame.
windowSpec.rowsBetween(start, end)
# Defines a Window Specification with a RANGE frame.
windowSpec.rangeBetween(start, end)

					SQL				DataFrame API
Ranking functions
              rank			rank
					dense_rank		denseRank
					percent_rank	percentRank
					ntile			      ntile
					row_number		rowNumber
Analytic functions
          cume_dist		cumeDist
					first_value		firstValue
					last_value		lastValue
					lag				    lag
					lead			      lead

Aggregate: 			min, max, avg, count, and sum.
Ranking:			rank, dense_rank, percent_rank, row_num, and ntile
Analytical: 		cume_dist, lag, and lead
Custom boundary: 		rangeBetween and rowsBetween
 */

object sparkWindowsFunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkWindowsFunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

//Create sample DataFrame
val columns=Seq("DepName","EmpNo","Salary")

    val empsalary = Seq(
      ("sales", 1, 5000),
      ("personnel", 2, 3900),
      ("sales", 3, 4800),
      ("sales", 4, 4800),
      ("personnel", 5, 3500),
      ("develop", 7, 4200),
      ("develop", 8, 6000),
      ("develop", 9, 4500),
      ("develop", 10, 5200),
      ("develop", 11, 5200))

      val df=spark.sparkContext.parallelize(empsalary).toDF(columns:_*)

    // Display DataFrame
    //df.show()

    // Create Window Specification for Aggregate Function
    val byDepName=Window.partitionBy($"depName")

    // Apply Aggregate Function on Window
     val agg_sal=df.withColumn("max_salary",max($"salary").over(byDepName))
                   .withColumn("min_salary",min($"salary").over(byDepName))
    agg_sal.select("depName","max_salary","min_salary").dropDuplicates().show()

    // Create Window Specification for Ranking Function
   val winSpec=Window.partitionBy($"depName").orderBy($"salary".desc)

    //Apply Rank Function
    //rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
     //This function will return the rank of each record within a partition and skip the subsequent rank following any duplicate rank:
    val rank_df=df.withColumn("Rank",rank().over(winSpec))
    rank_df.show()

    //Apply Dense Rank function
    //This function will return the rank of each record within a partition but will not skip any rank.
    //ense_rank() window function is used to get the result with rank of rows within a window partition without any gaps
    val dense_rnk=df.withColumn("Dense_Rank",dense_rank().over(winSpec))
    dense_rnk.show()

    //Row Number
    //row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.
    val row_num=df.withColumn("Row_number",row_number().over(winSpec))
    row_num.show()

    //percent_rank
    // rank+percent=percent rank (Skip rank when there is a duplicate)
    // This function will return the relative (percentile) rank within the partition.
    val percent_rnk=df.withColumn("percent_rnk",percent_rank().over(winSpec))
    percent_rnk.show()

    //ntile

    //This function can further sub-divide the window into n groups based on a window specification or partition. For example, if we need to divide the departments further into say three groups we can specify ntile as 3.
    //ntile() window function returns the relative rank of result rows within a window partition. In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)
    val ntile_df=df.withColumn("NTile",ntile(3).over(winSpec))
    ntile_df.show()

    //cume_dist
    //This function gives the cumulative distribution of values for the window/partition.
    val cume_disdf=df.withColumn("cume_dist",cume_dist().over(winSpec))
    cume_disdf.show()

    //lag
/*
    This function will return the value prior to offset rows from DataFrame.
    The lag function takes 3 arguments (lag(col, count = 1, default = None)),
    col: defines the columns on which function needs to be applied.
    count: for how many rows we need to look back.
    default: defines the default value.
    For depname = develop, salary = 4500. There is no such row which is 2 rows prior to this row. So it will get null.
    For deptname = develop, salary = 6000 (highlighted in blue). If we go 2 rows prior, we will get 5200 as salary (highlighted in green).*/
    val lagdf=df.withColumn("lag",lag($"salary",2).over(winSpec))
    lagdf.show()

   //lead
    /*
    This function will return the value after the offset rows from DataFrame.
lead function takes 3 arguments (lead(col, count = 1, default = None))
col: defines the columns on which the function needs to be applied.
count: for how many rows we need to look forward/after the current row.
default: defines the default value.
for depname = develop, salary = 4500 (highlighted in blue). If we go 2 rows forward/after, we will get 5200 as salary (highlighted in green).
 for depname = personnel, salary = 3500. There is no such row which is 2-row forward/after this row in this partition. so we will get null.
     */
    val leaddf=df.withColumn("lead",lead($"salary",2).over(winSpec))
    leaddf.show()

    /*
    By default, the boundaries of the window are defined by partition column and we can specify the ordering via window specification.
    But what if we would like to change the boundaries of the window?
    The following functions can be used to define the window within each partition.
    Using therangeBetween function, we can define the boundaries explicitly.
For example, let’s define the start as 100 and end as 300 units from current salary and see what it means. Start as 100 means the window will start from 100 units and end at 300 value from current value (both start and end values are inclusive).
For depname=develop, salary =4200, start of the window will be (current value + start) which is 4200 + 100 = 4300. End of the window will be (current value + end) which is 4200 + 300 = 4500.
Since there is only one salary value in the range 4300 to 4500 inclusive, which is 4500 for develop department, we got 4500 as max_salary for 4200
Similarly for depname=develop, salary = 4500, the window will be (start : 4500 + 100 = 4600, end : 4500 + 300 = 4800). But there are no salary values in the range 4600 to 4800 inclusive for develop department so max value will be null (check output above).
     */
    //RangeBetween
    val winSpec1=Window.partitionBy($"depName").orderBy($"salary").rangeBetween(100L,300L)
    val range_between_df=df.withColumn("max_sal_rangebet",max($"salary").over(winSpec1))
    range_between_df.show()

    /*
    There are some special boundary values which can be used here.
Window.currentRow: to specify a current value in a row.
Window.unboundedPreceding: This can be used to have an unbounded start for the window.
Window.unboundedFollowing: This can be used to have an unbounded end for the window.

So, for depname = personnel, salary = 3500. the window will be (start : 3500 + 300 = 3800, end : unbounded). So the maximum value in this range is 3900 (check output above).
Similarly, for depname = sales, salary = 4800, the window will be (start : 4800 + 300, 5100, end : unbounded). Since there are no values greater than 5100 for sales department, null results.
     */

    val winSpec2=Window.partitionBy($"depName").orderBy($"salary").rangeBetween(300L,Window.unboundedFollowing)
    val range_unbounded_df=df.withColumn("max_sal_rangeunboun",max($"salary").over(winSpec2))
    range_unbounded_df.show()

   //RowsBetween
    /*
    With rangeBetween, we defined the start and end of the window using the value of the ordering column. However, we can also define the start and end of the window with the relative row position.
For example, we would like to create a window where start of the window is one row prior to current and end is one row after current row.
For depname = develop, salary = 4500, a window will be defined with one row prior and after the current row (highlighted in green). So salaries within the window are (4200, 4500, 5200) and max is 5200 (check output above).
Similarly, for depname = sales, salary =5000, a window will be defined with one prior and after the current row. Since there are no rows after this row, the window will only have 2 rows (highlighted in green) which have salaries as (4800, 5000) and max is 5000 (check output above).

We can also use the special boundaries Window.unboundedPreceding, Window.unboundedFollowing, and Window.currentRow as we did previously with rangeBetween.
Note: Ordering is not necessary with rowsBetween, but I have used it to keep the results consistent on each run
     */
   val winSpec3=Window.partitionBy($"depName").orderBy($"salary").rowsBetween(-1,1)
    val rows_betwn_df=df.withColumn("max_sal_rowsbet",max($"salary").over(winSpec3))
    rows_betwn_df.show()


    spark.stop()
  }
}

/*
A window specification defines which rows are included in the frame associated with a given input row.A window specification includes three parts:

1.Partitioning Specification: controls which rows will be in the same partition with the given row. Also, the user might want to make sure all rows having the same value for  the category column are collected to the same machine before ordering and calculating the frame.  If no partitioning specification is given, then all data must be collected to a single machine.
2.
Ordering Specification: controls the way that rows in a partition are ordered, determining the position of the given row in its partition.
3.
Frame Specification: states which rows will be included in the frame for the current input row, based on their relative position to the current row.  For example, “the three rows preceding the current row to the current row” describes a frame including the current input row and three rows appearing before the current row.
=======================================================
from pyspark.sql.window import Window
windowSpec = \
  Window \
    .partitionBy(...) \
    .orderBy(...)
In addition to the ordering and partitioning, users need to define the start boundary of the frame, the end boundary of the frame, and the type of the frame, which are three components of a frame specification.

There are five types of boundaries, which are
1.UNBOUNDED PRECEDING
2. UNBOUNDED FOLLOWING
3.CURRENT ROW,
4.<value> PRECEDING
5.<value> FOLLOWING.

UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING represent the first row of the partition and the last row of the partition, respectively.
For the other three types of boundaries, they specify the offset from the position of the current input row andtheir specific meanings are defined based on the type of the frame.

 There are two types of frames, ROW frame and RANGE frame.
ROW frame
===========
ROW frames are based on physical offsets from the position of the current input row, which means that
 CURRENT ROW, <value> PRECEDING, or <value> FOLLOWING specifies a physical offset.
 If CURRENT ROW is used as a boundary, it represents the current input row.
 <value> PRECEDING and <value> FOLLOWING describes the number of rows appear before and after the
  current input row, respectively.The following figure illustrates a ROW frame with a 1 PRECEDING
  as the start boundary and 1 FOLLOWING as the end boundary (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING in the SQL syntax).

RANGE frame
=============
RANGE frames are based on logical offsets from the position of the current input row, and have
similar syntax to the ROW frame. A logical offset is the difference between the value of the ordering
 expression of the current input row and the value of that same expression of the boundary row of the
 frame. Because of this definition, when a RANGE frame is used, only a single ordering expression
 is allowed.Also, for a RANGE frame, all rows having the same value of the ordering expression
 with the current input row are considered as same row as far as the boundary calculation is concerned.

 In this example, the ordering expressions is revenue; the start boundary is 2000 PRECEDING; and the
 end boundary is 1000 FOLLOWING (this frame is defined as RANGE BETWEEN 2000 PRECEDING AND 1000
 FOLLOWING in the SQL syntax).The following five figures illustrate how the frame is updated with
  the update of the current input row.Basically, for every current input row, based on the value
  of revenue, we calculate the revenue range [current revenue value - 2000, current revenue value
  + 1000]. All rows whose revenue values fall in this range are in the frame of the current input row



 What are the best-selling and the second best-selling products in every category?
SELECT
  product,
  category,
  revenue
FROM (
  SELECT
    product,
    category,
    revenue,
    dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
  FROM productRevenue) tmp
WHERE
  rank <= 2
  ===============================
   “What is the difference between the revenue of each product and the revenue of the best selling product in the same category as that product?”

import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
windowSpec = Window .partitionBy(df['category']).orderBy(df['revenue'].desc()).rangeBetween(-sys.maxsize, sys.maxsize)
dataFrame = sqlContext.table("productRevenue")
revenue_difference =(func.max(dataFrame['revenue']).over(windowSpec) - dataFrame['revenue'])
dataFrame.select(dataFrame['product'],
  dataFrame['category'],
  dataFrame['revenue'],
  revenue_difference.alias("revenue_difference"))
 */