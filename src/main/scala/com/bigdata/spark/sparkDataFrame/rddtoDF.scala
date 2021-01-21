package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//why impilicit is activated even when there is no toDF
// it s because while performing operations like groupby(huge shuffle of data happens) catalyst optimiser internally select best optimised physical plan
// which is given Dataset(as dataframe is converted to dataset) which will be chosen so impilicit is automatically in active mode

/*
//Programmatically Specifying the Schema
======================================================================
*/
object rddtoDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql


    import org.apache.spark.sql.types._
    import org.apache.spark.sql

    val data = "D:\\bigdata\\datasets\\bank-full.csv"
    // years back old strategy
    val rdd = sc.textFile(data)
    //processing only schema
    val head = rdd.first() // header// age; balance, marital,job
    val fields = head.split(";").map(x => StructField(x.replaceAll("\"",""), StringType, nullable = true))
    /*
     fields: Array[org.apache.spark.sql.types.StructField] = Array(StructField(age,StringType,true), StructField(job,StringType,true), StructField(m
    arital,StringType,true), StructField(education,StringType,true), StructField(default,StringType,true), StructField(balance,StringType,true), St
    ructField(housing,StringType,true), StructField(loan,StringType,true), StructField(contact,StringType,true), StructField(day,StringType,true),
    StructField(month,StringType,true), StructField(duration,StringType,true), StructField(campaign,StringType,true), StructField(pdays,StringType,
    true), StructField(previous,StringType,true), StructField(poutcome,StringType,true), StructField(y,StringType,true))
*/
    val schema = StructType(fields)
/*
  schema: org.apache.spark.sql.types.StructType = StructType(StructField(age,StringType,true), StructField(job,StringType,true), StructField(mari   tal,StringType,true), StructField(education,StringType,true), StructField(default,StringType,true), StructField(balance,StringType,true), Struc
  tField(housing,StringType,true), StructField(loan,StringType,true), StructField(contact,StringType,true), StructField(day,StringType,true), Str
  uctField(month,StringType,true), StructField(duration,StringType,true), StructField(campaign,StringType,true), StructField(pdays,StringType,tru
  e), StructField(previous,StringType,true), StructField(poutcome,StringType,true), StructField(y,StringType,true))

 */
    // Convert records of the RDD (people) to Rows
    rdd.map(x=>x.replaceAll("\"","").split(";")).take(5)

 /* Array[Array[String]] = Array(Array(age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign
      , pdays, previous, poutcome, y), Array(58, management, married, tertiary, no, 2143, yes, no, unknown, 5, may, 261, 1, -1, 0, unknown, no), Arra
      y(44, technician, single, secondary, no, 29, yes, no, unknown, 5, may, 151, 1, -1, 0, unknown, no), Array(33, entrepreneur, married, secondary,
      no, 2, yes, yes, unknown, 5, may, 76, 1, -1, 0, unknown, no), Array(47, blue-collar, married, unknown, no, 1506, yes, no, unknown, 5, may, 92,
      1, -1, 0, unknown, no))
     */


    val rowRDD = rdd.map(x=>x.replaceAll("\"","").split(";")).map(x => Row.fromSeq(x))
 /*
 res1: Array[org.apache.spark.sql.Row] = Array([age,job,marital,education,default,balance,housing,loan,contact,day,month,duration,campaign,pdays
,previous,poutcome,y], [58,management,married,tertiary,no,2143,yes,no,unknown,5,may,261,1,-1,0,unknown,no], [44,technician,single,secondary,no,
29,yes,no,unknown,5,may,151,1,-1,0,unknown,no], [33,entrepreneur,married,secondary,no,2,yes,yes,unknown,5,may,76,1,-1,0,unknown,no], [47,blue-c
ollar,married,unknown,no,1506,yes,no,unknown,5,may,92,1,-1,0,unknown,no])

  */
    // Apply the schema to the RDD
    val df = spark.createDataFrame(rowRDD, schema)
    //df: org.apache.spark.sql.DataFrame = [age: string, job: string ... 15 more fields]
    /* df.printSchema
    root
 |-- age: string (nullable = true)
 |-- job: string (nullable = true)
 |-- marital: string (nullable = true)
 |-- education: string (nullable = true)
 |-- default: string (nullable = true)
 |-- balance: string (nullable = true)
 |-- housing: string (nullable = true)
 |-- loan: string (nullable = true)
 |-- contact: string (nullable = true)
 |-- day: string (nullable = true)
 |-- month: string (nullable = true)
 |-- duration: string (nullable = true)
 |-- campaign: string (nullable = true)
 |-- pdays: string (nullable = true)
 |-- previous: string (nullable = true)
 |-- poutcome: string (nullable = true)
 |-- y: string (nullable = true)
scala> val all=df.count.toInt
all: Int = 45212

scala> val all=df.count
all: Long = 45212
*/
    val all=df.count.toInt
    df.show(5)
    /*
    +---+------------+-------+---------+-------+-------+-------+----+-------+---+-----+--------+--------+-----+--------+--------+---+
|age|         job|marital|education|default|balance|housing|loan|contact|day|month|duration|campaign|pdays|previous|poutcome|  y|
+---+------------+-------+---------+-------+-------+-------+----+-------+---+-----+--------+--------+-----+--------+--------+---+
|age|         job|marital|education|default|balance|housing|loan|contact|day|month|duration|campaign|pdays|previous|poutcome|  y|
| 58|  management|married| tertiary|     no|   2143|    yes|  no|unknown|  5|  may|     261|       1|   -1|       0| unknown| no|
| 44|  technician| single|secondary|     no|     29|    yes|  no|unknown|  5|  may|     151|       1|   -1|       0| unknown| no|
| 33|entrepreneur|married|secondary|     no|      2|    yes| yes|unknown|  5|  may|      76|       1|   -1|       0| unknown| no|
| 47| blue-collar|married|  unknown|     no|   1506|    yes|  no|unknown|  5|  may|      92|       1|   -1|       0| unknown| no|
+---+------------+-------+---------+-------+-------+-------+----+-------+---+-----+--------+--------+-----+--------+--------+---+
only showing top 5 rows
     */
    df.createOrReplaceTempView("bank")

    val res=spark.sql("select * from bank where age>90")
    val res1=spark.sql("select * from banktab where loan=\"yes\" and age>60")

    //res: org.apache.spark.sql.DataFrame = [age: string, job: string ... 15 more fields]
    res.show()
    res1.show()
    spark.stop()
  }
}

/*When case classes cannot be defined ahead of time
(for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users),
, a DataFrame can be created programmatically with three steps.

-Create an RDD of Rows from the original RDD;
-Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
-Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
=================
 A [[StructType]] object can be constructed by
 StructType(fields: Seq[StructField])
 * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
 * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
 * If a provided name does not have a matching field, it will be ignored. For the case
 * of extracting a single [[StructField]], a `null` will be returned.
 *
 * Scala Example:
 * {{{
 * import org.apache.spark.sql._
 * import org.apache.spark.sql.types._
 *
 * val struct =
 *   StructType(
 *     StructField("a", IntegerType, true) ::
 *     StructField("b", LongType, false) ::
 *     StructField("c", BooleanType, false) :: Nil)
 *
 * // Extract a single StructField.
 * val singleField = struct("b")
 * // singleField: StructField = StructField(b,LongType,false)
 *
 * // If this struct does not have a field called "d", it throws an exception.
 * struct("d")
 * // java.lang.IllegalArgumentException: Field "d" does not exist.
 * //   ...
 *
 * // Extract multiple StructFields. Field names are provided in a set.
 * // A StructType object will be returned.
 * val twoFields = struct(Set("b", "c"))
 * // twoFields: StructType =
 * //   StructType(StructField(b,LongType,false), StructField(c,BooleanType,false))
 *
 * // Any names without matching fields will throw an exception.
 * // For the case shown below, an exception is thrown due to "d".
 * struct(Set("b", "c", "d"))
 * // java.lang.IllegalArgumentException: Field "d" does not exist.
 * //    ...
 * }}}
 *
 * A org.apache.spark.sql.Row object is used as a value of the StructType.
 *
 * Scala Example:
 * {{{
 * import org.apache.spark.sql._
 * import org.apache.spark.sql.types._
 *
 * val innerStruct =
 *   StructType(
 *     StructField("f1", IntegerType, true) ::
 *     StructField("f2", LongType, false) ::
 *     StructField("f3", BooleanType, false) :: Nil)
 *
 * val struct = StructType(
 *   StructField("a", innerStruct, true) :: Nil)
 *
 * // Create a Row with the schema defined by struct
 * val row = Row(Row(1, 2, true))
 * }}}
 *
 * @since 1.3.0

@InterfaceStability.Stable
case class StructType(fields: Array[StructField]) extends DataType with Seq[StructField] {

=================StructField=========
* A field inside a StructType.
* @param name The name of this field.
* @param dataType The data type of this field.
* @param nullable Indicates if values of this field can be `null` values.
* @param metadata The metadata of this field. The metadata should be preserved during
*                 transformation if the content of the column is not modified, e.g, in selection.
*

case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty)
* @since 1.3.0
*/
