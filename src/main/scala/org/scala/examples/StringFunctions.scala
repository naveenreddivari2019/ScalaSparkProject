package org.scala.examples

object StringFunctions extends App{

  val str:String ="Hello Welcome to Scala"
  println(str.length)

  val name:String ="Naveen"
  val age:Int =39

  val result1=s"My name is $name and my age is ${age+1}"
  println(result1)

  val donut:String = "Burger"
  val price:Float =252.968f
  val fInterpolate=f"Donut name is $donut%s and price is $price%2.2f"
  println(fInterpolate)

  val rawInterpolate=raw"My name is \n $name"
  println(rawInterpolate)


}
