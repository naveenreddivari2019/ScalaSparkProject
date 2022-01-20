package org.scala.examples

import scala.annotation.tailrec

object Recursion extends App{

  def factorailWithoutTail(n:Int):Int={
    if(n<=1) 1
    else
      n * factorailWithoutTail(n-1)
  }

  def factorialWithTail(n:Int):BigInt={
    @tailrec
    def factorailHelper(x:Int,accumulator:BigInt):BigInt={
      if(x<=1) accumulator
      else {
        factorailHelper(x-1, x*accumulator)
      }
    }
    factorailHelper(n,1)
  }

  println(factorialWithTail(5000))
}
