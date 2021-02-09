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



