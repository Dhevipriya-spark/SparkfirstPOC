val name="ravi"
val age=27
if(age >18) s"Hi $name your age is $age so you will get $age %discount"
else s"Hi $name you will get chess and carrom board"

//nested if else
val res=if(age>0 && age <13 ) "J&J products 50% off"
else if(age>=13 && age <25) "Books 50% discount"
else if(age>=25 && age<60 )  " Mobiles and Health insurance 50% dicount"
else "no offer"

//create function
//define function with def keyword
//hide the logic,code is reusable,No side effects
def offer(age:Int)={
  if(age>0 && age <13 ) "J&J products 50% off"
  else if(age>=13 && age <25) "Books 50% discount"
  else if(age>=25 && age<60 )  " Mobiles and Health insurance 50% dicount"
  else "no offer"
}

offer(43)
offer(16)
offer(12)
offer(70)
///////////////////////////////////////////
//match almost like switch case
val day="sunday"

//usecase1
// =>  implies (do some action)
val res=name match{
  case "ravi" => s"Hello $name,please complete reporting task"
  case "venu" => s"Hello $name,please complete developer task"
  case "vijay" => s"Hello $name,please complete admin task"
  case _ =>"you are not a member of this project"
}

def emptask(name:String)= name match {
  case "ravi" => s"Hello $name,please complete reporting task"
  case "venu" => s"Hello $name,please complete developer task"
  case "vijay" => s"Hello $name,please complete admin task"
  case _ => "you are not a member of this project"
}
  emptask("venu")
  emptask("vijay")


val off=day.toLowerCase match{
  case "sun" | "sunday" | "sat" |"saturday" => "Weekend Offer"
  case "monday" | "Mon" | "Tuesday" |"Tue" | "Wednesday" | "Wed" => "Weekstart Offer"
  case "Thurday" | "Thur" | "Friday" |"Fri" => "Weekend offer"
}

def offer(day:String) =day.toLowerCase match{
  case "sun" | "sunday" | "sat" |"saturday" => "Weekend Offer"
  case "monday" | "mon" | "tuesday" |"tue" | "wednesday" | "Wed" => "Weekstart Offer"
  case "thurday" | "thur" | "friday" |"fri" => "Weekdayclosing offer"
}
offer("mon")
offer("fri")

//usecase3
//Especially between a range if want to process use a safeguard(x,age)
val name1="modi"
val age1=29
def agerange(age1:Int)= age1 match {
  case x if(x>0 && x<18) => "J&J free"
  case x if(x>=18 && x<30) => "mobile free"
  case x if(x>=30 && x<60) => "laptop free"
  case _ => "No offer"
}

agerange(12)
agerange(35)
agerange(79)

//usecase4
val det =("venu",32,"m")
def genageoff(det:(String,Int,String))=det match{
  case (n,a,g) if(a>18 && g=="f" && age<=35) =>s"hi $n you will get free Lipstick"
  case(n,a,g) if(a>18 && g=="m") => s"Hi $n you will get shaving kit free"
  case _ => "no offer"
}

genageoff(det)
genageoff("venu",17,"m")
genageoff("venkat",19,"m")
genageoff("renu",17,"f")
genageoff("renu",47,"f")
genageoff("renu",27,"f")
