package com.rockthejvm.part3Concurrency

import cats.effect.{IO, IOApp}

import scala.concurrent.duration.*
import com.rockthejvm.utils.*

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object p5_BlockingIOs extends IOApp.Simple {

  val someSleeps = for {
    _ <- IO.sleep(1.second).debug //SEMANTIC BLOCKING
    _ <-IO.sleep(1.second).debug
  } yield ()

  // REALLY (non semantic) blocking ios
  val aBlockingIO = IO.blocking{
   Thread.sleep(1000)
    println(s"[${Thread.currentThread().getName}] computed a blocking code")
    42
  }
  // Will evaluate on a thread from ANOTHER pool which is specific for blocking calls


  //yielding
  val iosOnManyThreads = for {
    _ <- IO("first").debug
    _ <- IO.cede // a signal to yield control over the thread - equivalent to IO.shift on CatsEffect2
    _ <- IO("second").debug
    _ <- IO.cede
    _ <- IO("third").debug
  } yield ()

  private def testThousandEffectsSwitch(): IO[Int] = {
    val ex: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
    (1 to 1000).map(IO.pure).reduce(_.debug >> IO.cede >> _.debug).evalOn(ex)
  }

  override def run = //someSleeps
  //aBlockingIO.debug.void
  //iosOnManyThreads
  testThousandEffectsSwitch().void
}
