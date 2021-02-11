/*//Scala is a scalable language
//Match & if else will process only one element

//To process collection of elements  use for loop
//Like No need to declare temporary variable or no need to increment it
//i is temp variable,names is a collection of element ,if u want to do anything only process i
//By default for loop returns unit
//<- means pass all elements to x .. by default for looop return unit .. means after for loop no further processing.
//for (<identifier> <- <iterator>) [yield] [<expression>]*/

//To comment/uncomment a piece of code  ..select the code and enter shift +ctrl+?
val names=Array("venu","ravi","priya","anurag","shiva")

for(i<-names)  println(i)
for(i<-names)(i) //no result
val res=for(i<-names)(i)
res  ////no result
val res1=for(i<-names) yield i
res1
val nums =1 to 20 toArray
val res=for(x <- nums) println(x)

for (x <- 1 to 7) yield x
//res12: scala.collection.immutable.IndexedSeq[Int] = Vector(1, 2, 3, 4, 5, 6, 7)
val nums1 = -10 to 20 toArray

val res = for (x<- nums1 if x>0) yield x match {
  case a if(a%2==0) => a*a
  case _ => x*x*x
}
// based on business req u r using if , match, for loop together and solve
val num1 = 1 to 10 toArray
val num2 = 1 to 20 toArray
val re = for(x<-num1;y<-num2) println(s"$x * $y = ${x*y}")

def table(a:Int)={
  val num = 1 to 5 toArray;
  for(x<-num) yield(s"$a * $x=${a*x}")
}
table(5)


def mularr(num1:Array[Int],num2:Array[Int])={
  for(x<-num1;y<-num2)println(s"$x *$y=${x*y}")
}

def sqr(x:Int)=x*x
sqr(6)
sqr(9)

//Afunction calling itself is called recursive function
//recursive function  must specify return datatype

def fact(a:Int):Int =a match{
  case x if (x<=0) =>1
  case _ => a* fact(a-1)
}
fact (5)


