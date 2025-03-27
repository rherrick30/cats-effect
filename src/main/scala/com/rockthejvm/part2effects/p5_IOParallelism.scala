package com.rockthejvm.part2effects

import cats.{Parallel, PartialOrder}
import cats.effect.{IO, IOApp}

import cats.syntax.apply.*

import com.rockthejvm.utils.*

object p5_IOParallelism extends IOApp.Simple {

  // IOs are usually sequential
  val aniIO = IO(s"[${Thread.currentThread().getName}] Ani")
  val cameronIO = IO(s"[${Thread.currentThread().getName}] Cameron")

  val composedIO = for {
    ani <- aniIO
    cam <- cameronIO
  } yield s"$ani and $cam love Rock The JVM"

  // these two are sequential
  val meaningOfLife : IO[Int] = IO.delay(42)  //.debug
  val faveLang : IO[String] = IO.delay("It's Scala") //.debug
  val goalInLife = (meaningOfLife.debug, faveLang.debug).mapN((num, string) => s"my goal in life is $num and $string")

  // these are parallel
  val parIO: IO.Par[Int] = Parallel[IO].parallel(meaningOfLife.debug)
  val parIO2: IO.Par[String]  =  Parallel[IO].parallel(faveLang.debug)
  // import implicits (done below) to create a context for the threaded run
  import cats.effect.implicits.*
  val parallelGoals = (parIO, parIO2).mapN((num, string) => s"my goal in life is $num and $string")
  // convert back to sequential...
  val goalInLife_v2 = Parallel[IO].sequential(parallelGoals)

  // shorthands
  import cats.syntax.parallel.*
  // parMapN does the same thing as mapN, but it automatically converts to IO.Par[A]: note whats in the tuple
  val goalInLife_v3: IO[String] = (meaningOfLife.debug, faveLang.debug).parMapN((num, string) => s"my goal in life is $num and $string")

  // regarding failure..
  val aFailure: IO[String] = IO.raiseError(new RuntimeException("I can't do this"))
  // compose suceess and failure
  val parallelWithFailure = (meaningOfLife.debug, aFailure.debug).parMapN((msg, fail) => s"mesage is $msg and failure is $fail")
  // if one fails...they all fail
  val anotherFailure: IO[String] = IO.raiseError(new RuntimeException("Curses, foiled again"))
  val parallelWithFailure_v2 = (aFailure, anotherFailure  ).parMapN((f1, f2)=>s"failed $f1 ; failed again $f2")
  // the first io that fails, is the only one you'll see...thats "aFailure" in the above...
  val twoFailuresDelayed : IO[String] = (IO(Thread.sleep(1000)) >> aFailure.debug, anotherFailure.debug).parMapN((f1, f2)=>s"failed $f1 ; failed again $f2")
  // ...and "anotherFailure" here due to the Sleep-ing



  def run : IO[Unit] =
    //composedIO.map(println)
    //goalInLife.map(println)
    //goalInLife_v2.map(println)
    //goalInLife_v2.debug.void // to see the thread which this is executed
    //goalInLife_v3.map(println)
    //parallelWithFailure.map(println)
    //parallelWithFailure_v2.map(println)
    twoFailuresDelayed.map(println)
}
