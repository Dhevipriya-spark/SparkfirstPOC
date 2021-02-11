package com.bigdata.spark.sparkRDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("firsttest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

   /* println("hello world")
    //val num =1 to 10 toArray
    //val nrdd=sc.parallelize(num)
    //val nrdd=spark.sparkContext.parallelize(num)
    //val res =nrdd.map(x=>x*x).filter(x=>x<20).collect.foreach(println)

    val nums=Array(1,2,3,4,5,6)
    val nrdd1=sc.parallelize(nums)
    def sqr(x:Int) = x*x
    val ftr=(x:Int) => x<20
    val res1 =nrdd1.map(sqr _).filter(ftr)
    res1.take(5).foreach(println)
    //res1.collect.foreach(println)*/

    val data="D:\\BigData\\Datasets\\asl.csv"
    val aslrdd=sc.textFile(data)
    //select * from table where city =
    //select city,count(*) from table group by city //reduceByKey or grouBbyKey
    //select join two tables
    //Below approach is bad as even "name" with "hyd" string in it would get displayed
    // but our intention is to print city with hyd names


    //Encapsulate all the elements in the array and give the referencing number
    val res2=aslrdd
    res2.foreach(println)

   //val res=aslrdd.filter(x=>x.contains("hyd"))
    //val res1=aslrdd.filter(x=>x.contentEquals("mas")) ???
   //res.collect.foreach(println)
  // So when we need to query only on city column or apply logic only to city columns ..we split by comma
    //use map as it applies logic to each and every element
    //Split returns Array[string]
    val res1=aslrdd.map(x=>x.split(","))  // returns RDD[Array[String]]
    res1.collect.foreach(println)
    //In intellij it returns an array referencing address with only(split) but terminal this works
    //Array(jyo,12,blr,fri)
    //Array(anu,49,mas,tue)

    //Converting Array to tuple -use map // returns RDD[(String,String,String,String)]
    //Tuple is used to handle multiple data types
    //val resfinal= aslrdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3)))

    //filter is an equivalent of where in sql
    //select rows with hyd city columns
    //val resfinal3= aslrdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3))).filter(x=>x._3 =="hyd")
    //resfinal3.collect.foreach(println)
    //(venu,32,hyd,monday)

    //returns RDD[(String,Int,String,String)]
    //val resfinal1= aslrdd.map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3)))

    //Throws error becoz Numberformat exception :for input string "age" As column names are intact (representation of data should be cleaned)
    //val resfinal1= aslrdd.map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3))).filter(x=>x._3 =="blr" && x._2<30)

    //Removes the first line in the file
    val head=aslrdd.first() //Action -returning value
    val result =aslrdd.filter(x=>x!=head)
    result.collect.foreach(println)

    //select * from tab where state=city and age<30
    val resfinal1= aslrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3))).filter(x=>x._3 =="blr" && x._2<30)
    resfinal1.collect.foreach(println)
    resfinal1.toDebugString
    //frontend sc.textfile backend calling hadooprdd..//frontend map..filter backend mappartitionrdd
     //spark uses HAdoop RDD API to read the data from any hadoop supported filesystem.
    //HadoopRDD- if any sc.textfile is present that sc.textfile calls hadooprdd API
    // Spark uses hadoopRDD API to read any hadoop supported filesystem file & get the data from this path and creates hadooprdd.
    // then sc.textfile creates mappartitionRDD.then for filter map etc mappartitionRDD gets created...
    //Returns RDD[String]
    val resflat= aslrdd.flatMap(x=>x.split(","))

    //print char at n th position
    val res3=aslrdd.map(x=>x.charAt(1))
    val res4=aslrdd.map(x=>x.charAt(0))
    res3.collect.foreach(println)
    res4.collect.foreach(println)
   //=>Apply logic on each and every element

    //Use toDebugString =>ShuffleRDD gets created=> processing is done in Stages.
    //reducebykey -a new stage gets created.
    //MapPartitionsRDD=All data gets processsed in the same Executors in the same machine.
    // ShuffledRDD==To do further processing data(datauntil before the the reducebykey) gets tranferred from one executor to another executor(different node)
    //And return the results to the driver
    //ShuffleRDD is also API .Increase the stages means decresesd the performance
    //standalone cluster-1 executor..data goes to local temp dir.shufflinf with in the same computer
    val res=aslrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3))).map(x=>(x._3,1)).reduceByKey((a,b)=>a+b)
    res.collect().foreach(println)

    //Map (no of input and output elements  to a map are same) scala code
   /* val nums= 1 to 5 toArray
     nums.map(x=>x*x) //output has 5 elements
      nums.map(x=>x>3)//output has 5 elements
    //filter(no of input and output elements need not be same)
    val nums=1 to 15 toArray
      nums.filter(x=>x>3) //input 15 elements output 12
      nums.filter(x=>x>10 && x<15) //input 15 output14
      //Flatmap -flattens a nested array into an array
      > val names=Array("venu modi","narendra modi","neerav modi","Dhevipriya modi")
       names: Array[String] = Array(venu modi, narendra modi, neerav modi, Dhevipriya modi)
      > names.map(x =>x.split(" "))
      res0: Array[Array[String]] = Array(Array(venu, modi), Array(narendra, modi), Array(neerav, modi), Array(Dhevipriya, modi))
      > names.map(x =>x.split(" ")).flatten
       res1: Array[String] = Array(venu, modi, narendra, modi, neerav, modi, Dhevipriya, modi)



*/
    val names=Array("Dhevipriya vijay","veeana vijay")
    //names: Array[String] = Array(Dhevipriya vijay, veeana vijay)
    val nmrdd=sc.parallelize(names)
      //nmrdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize
    nmrdd.map(x=>x.split(" "))
      //res2: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[1] at map
    nmrdd.map(x=>x.split(" ")).collect
    // Array[Array[String]] = Array(Array(Dhevipriya, vijay), Array(veeana, vijay ))
    //nmrdd.map(x=>x.split(" ")).flatten.collect
    //<console>:26: error: value flatten is not a member of org.apache.spark.rdd.RDD[A
    nmrdd.flatMap(x=>x.split(" ")).collect
          //res5: Array[String] = Array(Dhevipriya, vijay, veeana, vijay)

    spark.stop()
  }
}
//RDD API ->usually unstructured/structured.It is programming friendly
//Dataframe API ->Structured/semistructured data programming and DB friendly
//Dataset API ->Any type of structured/unstructured/semistructured/live data
//Dstream API->Only live data
//Map is used to Apply logic to a particular column,field,row.
//Flatmap is used to apply logic on all the data
//

//RDD means its always in memory
//Dependency between multiple transformations in the form of graph is called lineage graph
//aslrdd<==filter==>map<==map<==filter<==Collect
//===>dependent
//Check DAG visualisation available -Details of stages ==>
//if any groupbykey or reducebykey is used .then if the partitions of the data are in different executors
//then the data gets reduced accordingly and result get returned to the driver.

//if there are 100 maps 200filters each map 10mb memory then 1 gb map and 2gb filter
//so in all3 gb memory used .so lot of memory is consumed.so as spark is doing the computation stage by stage
//Allthe maps ..filter if they in lakhs are processed ina second with in a stage.so the resources are utilised effectively
//that  is why spark is processing stagebystage and not method by method i.e combining transformations.

//Map=Apply a logic on top of each and every element.No of input and output elements length must be same
//Mapusually apply a logic on top of specific columns..array convert to tuple use map.
// Map is processed within the same computer.so there is no performance reduction

//filter:based on boolean value. apply a logic based on eacha nd every element.Return a new RDD containing only the elements that satisfy a predicate...no of input and output element
//need not be same.It is almost like where clause in sql select * from tab where
//first -Its a action that returns a value

//flatmap= Return a new rdd first by Applying a function on top of each and every element of this rdd and then flatten the result
//flatmap =flat +map Flatten. map is using map and flatten internally.No of elements in input and no of output elements need not
//be same.usecase is unstructured data.Rule is rule..rule for all.Apply a logic on all columns

//Action cannot be called within a transformation directly.so declare the variable outside and then refer it within a transformation
//val head=aslrdd.first() //Action -returning value
//val result =aslrdd.filter(x=>x!=head)
//Else ignore the first row if it has age in it. rdd.filter(x=>x(0) != "\"age\"")  [age]
 //csvrdd.map(Split(,)).filter(x=>x(1)!="age").map(x=>(x(0),x(1)...)
//map(split)==> Returns Array(ele1,ele2,ele3)
//flatmap(split)==>Returns (ele1,ele2,ele3)