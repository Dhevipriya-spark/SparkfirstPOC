val x=99
//x=55 Error
var y=25
y=40
var y:String="Dp"
val x:String="Vijay"
//y=88 Error

//Name/Identifier must start with character
//val 50ce="Venu" //Invalid literal number
val abc ="veeana"
val _3=555
val $aa="Dp"
//As per scala std ,Anything after . is considered as method
//so here .b is not a method .we are only decalring a variable.so wtihin inverted quotes
//val a.b ="pooja"
val `a.b` ="pooja"


val msg1="Hi Dp\n,How are you ? \n Are you doing good "

val msg=
  """ Hi vijay
    |How are you
    |I am doing good
    |""".stripMargin


//String  interpolation
//$ refers something -you are giving hint to scala compiler that it is string interpolation
val name="Vijay"
val age =60
val name1="Veeana"
val age1=3
val  developer= s"Hi $name,Are you a developer of $age,you will retire in ${age+5}"
val  developer1= s"""Hi $name1,Are you a developer of $age1,you will retire in ${age1+5}"""

//copy all sql squeries in """ Triple quotes

//Array //Collection of same datatypes - can be declared with val and var as it is mutable
//It is possible to change the data but not the datatype
//array starts from 0
val arr=Array(1,2,3,4,5,65 ) //Collection of same datatypes
var arr1=Array("Dp","Vijay")
arr(0)
arr(1)=50
//arr(2)="Dp" you cannot change the datatype
//Python it is possible to change the datatype as python  is a dynamic language
//But is a static language

val nums =1 to 20 toArray
val nums1 =1 to 20 by 2 toArray

val arr1=Array("Dp",31)  //arr1: Array[Any] = Array(Dp, 31)
val arr2=Array("Dp",5>2,50,false) //arr2: Array[Any] = Array(Dp, true, 50, false)
arr1(1)=25
//if(arr1(1)>18) "you are a major" else "minor" //value > is not a member of Any

nums.length
nums.min
nums.max
nums.filter(x=>x<6)
//Tuple means collection of different Datatypes(multi datatypes)
val tup=("venu",32,"mas")
tup._1
tup._2
tup._3

if(tup._2>18) "you are major" else "minor"
tup

///////////////////Expresssions -return something////////////
val blx ={
  val x=10
  val y=5
  x/y
  y/x
}

val blr ={
  val fname ="Dp"
  val lname="Marimuthu"
  fname + "" + lname
}

//Type hierarchy
//byte ->short->int-> long->->float->Double ->AnyVal
//char->int
//Boolean->Anyval
//String-->AnyRef
// List->Seq->Iterable->our scala classes  allpoint to ScalaObject -(All pointing to)->AnyRef
//Casting is unidirectional.
6/2 //3
7/2 //3 //processing of integer and integer will give integer
9/3.0 //Double //procesing of integer& double will give double

val b:Byte =44
val b=44.toByte
val b=44:Byte
val arr=Array(55:Byte,44:Byte,-22:Byte) //Array[Byte]
val arr=Array(55:Byte,44:Byte,-22:Byte ,666:Short) //Array[Short]Colection of Byte & short datatypes will change to Short
val arr=Array(55:Byte,44:Byte,-22:Byte ,666:Short,99:Int) //Array[Int]
val arr=Array(55:Byte,44:Byte,-22:Byte ,666:Short,99:Int,7899909L)//Array[Long]
val arr=Array(55:Byte,44:Byte,-22:Byte,88.4f) //Array[Float]
val arr=Array(55:Byte,44:Byte,-22:Byte,88.4f,99.9)  //Array[Double]
val arr=Array(55:Byte,'a','b',55) //char+integer+byte //arr: Array[Int] = Array(55, 97, 98, 55
val arr=Array(55:Byte,'a','b',55,77>9) //arr: Array[AnyVal] = Array(55, a, b, 55, true)
val arr=Array(55:Byte,'a','b',55,77>9,34) //arr: Array[AnyVal] = Array(55, a, b, 55, true, 34)
val test=for(x <- nums) x //test: Unit = ()
//Unit does notreturn anyvalue
//test.map(x=>x*x) //value map is not a member of unit
//test.filter(x=>x>7)
val arr=Array(55:Byte,'a','b',55,77>9,34,test)
//arr: Array[AnyVal] = Array(55, a, b, 55, true, 34, ())

//List is a collection of same datatype but immutable But array mutable
//List also starts from 0
val lst =1 to 30 by 2 toList
  val l1=lst(1)
//lst(0)=3 //value update is not a member of List[Int]
val lst1=List("a string",732,'c',true,() => "an anonymous function returning a string")
lst1.foreach(x=>println(x))
// //lst1: List[Any] = List(a string, 732, c, true, <function0>)
/*It defines a value list of type List[Any]. The list is initialized with
 elements of various types, but each is an instance of scala.Any, so you can
 add them to the list.
*/

val numbers=List(23,45,45,67,78)
var total = 0; for (i <- numbers) { total += i }
val total=numbers.reduce((x:Int,y:Int)=>x+y)
val f=List(23,8,14,21) filter(_>18)


val colors=List("red","green","blue")
for(c <- colors) {
  println(c)
}
colors.foreach((c:String)=>println(c))
colors.map((c:String)=>(c,c.size))

val numbers = 1 :: 2 :: 3 :: Nil
val first = Nil.::(1)
first.tail
val second=2::first
second.tail==first

//List Arithmetic
val f = List(23, 8, 14, 21) filter (_ > 18)
val p = List(1, 2, 3, 4, 5) partition (_ < 3)
val s = List("apple", "to") sortBy (_.size)
//==== Mapping Lists
List(0, 1, 0) collect {case 1 => "ok"}
List("milk,tea") flatMap (_.split(','))
List("milk","tea") map (_.toUpperCase)
val answer = List(11.3, 23.5, 7.2).reduce(_ + _)
//==== Reducing Lists
val validations = List(true, true, false, true, true, true)
val valid1 = !(validations contains false)
val valid2 = validations forall (_ == true)
val valid3 = validations.exists(_ == false) == false

//==== Pattern Matching With Collections
val statuses = List(500, 404)
val msg = statuses.head match {
  case x if x < 500 => "okay"
  case _ => "whoah, an error"
}
val msg = statuses match {
  case List(404, 500) => "not found & error"
  case List(500, 404) => "error & not found"
  case List(200, 200) => "okay"
  case _ => "not sure what happened"
}

val code = ('h', 204, true) match {
  case (_, _, false) => 501
  case ('c', _, true) => 302
  case ('h', x, true) => x
  case (c, x, true) => {
    println(s"Did not expect code $c")
    x
  }
}


val appended = List(1, 2, 3, 4) :+ 5
val suffix = appended takeRight 3
val middle = suffix dropRight 2

//set  ===>unique: scala.collection.immutable.Set[Int] = Set(10, 20, 30)
val unique=Set(10,20,30,20,20,10)
unique.foreach((x:Int)=>println(x))

//Map //colorMap: scala.collection.immutable.Map[String,Int] = Map(red -> 16711680, green -> 65280, blue -> 255)
val colorMap = Map("red" -> 0xFF0000, "green" -> 0xFF00,"blue" -> 0xFF)
val redRGB=colorMap("red")
val hasWhite=colorMap.contains("green")
for (pairs <-colorMap) {println(pairs)}


//Seq :List +Tuple
//seq is collection of tuples but seq returns List
//In Seq element order very important(first is string,second is integer...)
//it is also immutable

val tup=("venu",32,"mas")
val se=Seq(("venu",32,"mas"),("Dp",32,"mas"),("vj",32,"mas"))
//se: Seq[(String, Int, String)] = List((venu,32,mas), (Dp,32,mas), (vj,32,mas))

se(0) //res15: (String, Int, String) = (venu,32,mas)

//Mutable collections
//Map
val m = Map("AAPL" -> 597, "MSFT" -> 40)
val n = m - "AAPL" + ("GOOG" -> 521)
println(m)
println(n)

//Stream
def inc(i: Int): Stream[Int] = Stream.cons(i, inc(i+1))
val s = inc(1) //s: Stream[Int] = Stream(1, ?)
val l = s.take(5).toList //l: List[Int] = List(1, 2, 3, 4, 5)
 s//res30: Stream[Int] = Stream(1, 2, 3, 4, 5, ?)


def inc(head: Int): Stream[Int] = head #:: inc(head+1)
inc(10).take(10).toList //res31: List[Int] = List(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)

def to(head: Char, end: Char): Stream[Char] = (head > end) match {
case true => Stream.empty
case false => head #:: to((head+1).toChar, end)
}
val hexChars = to('A', 'F').take(20).toList
//hexChars: List[Char] = List(A, B, C, D, E, F)

//option collection
/*A function that returns a value wrapped in the Option collection is signifying that it
may not have been applicable to the input data, and as such may not have been able to
return a valid result. It offers a clear warning to callers that its value is only potential,
and ensures that its results will need to be carefully handled. In this way, Option provides
  a type-safe option for handling function results*/
var x: String = "Indeed"
var a = Option(x)
x = null
var b = Option(x)

def divide(amt: Double, divisor: Double): Option[Double] = {
  if (divisor == 0) None
  else Option(amt / divisor)
}

val legit = divide(5, 2)
val illegit = divide(3, 0)

val odds = List(1, 3, 5)
val firstOdd = odds.headOption
val evens = odds filter (_ % 2 == 0)
val firstEven = evens.headOption

//Try Collection
def loopAndFail(end: Int, failAt: Int): Int = {
  for (i <- 1 to end) {
    println(s"$i) ")
    if (i == failAt) throw new Exception("Too many iterations")
  }
  end
}
loopAndFail(10, 3)
val t1 = util.Try( loopAndFail(2, 3) )
val t2 = util.Try{ loopAndFail(4, 2) }
/*

Any is the supertype of all types, also called the top type. It defines certain universal
 methods such as equals, hashCode, and toString. Any has two direct subclasses: AnyVal and AnyRef.

AnyVal represents value types.
 There are nine predefined value types and they are non-nullable: Double, Float, Long, Int,
 Short, Byte, Char, Unit, and Boolean. Unit is a value type which carries no meaningful
 information. There is exactly one instance of Unit which can be declared literally like
 so: ().
 All functions must return something so sometimes Unit is a useful return type.

AnyRef represents reference types. All non-value types are defined as reference types.
Every user-defined type in Scala is a subtype of AnyRef. If Scala is used in the
 context of a Java runtime environment, AnyRef corresponds to java.lang.Object.

 Nothing and Null
Nothing is a subtype of all types, also called the bottom type. There is no value that has type Nothing.
A common use is to signal non-termination such as a thrown exception, program exit, or an infinite loop
(i.e., it is the type of an expression which does not evaluate to a value, or a method that does not return normally).

Null is a subtype of all reference types (i.e. any subtype of AnyRef). It has a single value
identified by the keyword literal null. Null is provided mostly for interoperability
with other JVM languages and should almost never be used in Scala code
 */

