package com.rockthejvm.part1recap

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Essentials {

  // I skipped most of this as it's pretty basic

  // Futures
  val tp = Executors.newFixedThreadPool(8)
  given ex : ExecutionContext = ExecutionContext.fromExecutorService(tp)
  val aFuture = Future {
    Thread.sleep(1000)
    println("dun")
    42
  }


  //on complete
  aFuture.onComplete {
    case Success(value) => println(s"The meaning of life is $value")
    case Failure(exception) => println(s"failed with ${exception}")
  }


  val anotherFuture = aFuture.map(_ + 1)
  anotherFuture.map(f2 => println(s"another future is $f2"))

  // partial functions are based on pattern matching
  val aPartialFunction: PartialFunction[String, Int] = {
    case "1" => 43
    case "2" => 56
    case _ => -1
  }

  List(1,2,3).foreach(
    i =>  println(s"The partial function of $i is ${aPartialFunction(i.toString)}"
    ))


   // need this to make the process complete
   val exitingFuture = Future{
     Thread.sleep(5000)
     System.exit(1)
   }

  def main(args: Array[String]): Unit = {
    println("bye now!")
  }

}
