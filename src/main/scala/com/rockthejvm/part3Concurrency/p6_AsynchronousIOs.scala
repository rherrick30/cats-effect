package com.rockthejvm.part3Concurrency

import cats.effect.{Async, IO, IOApp}

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.*
import com.rockthejvm.utils.*

object p6_AsynchronousIOs extends IOApp.Simple {

  // IOs can run asynchronously on fibers, without having to manually manage the fiber lifecycle
  val threadPool = Executors.newFixedThreadPool(8)
  given ec : ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  type Callback[A] = Either[Throwable, A] => Unit
  def computeMeaningOfLife(): Int = {
    Thread.sleep(1000)
    println(s"[${Thread.currentThread().getName}] computing the meaning of life on some other thread")
    42
  }

  def computeMeaningOfLifeEither() : Either[Throwable, Int] = Try(computeMeaningOfLife()).toEither

  def computeMOLOnThreadPool(): Unit = {
    threadPool.execute(() => computeMeaningOfLife())
  }

   // Lift an async computation to an IO
  // async is an FFI (foreign function interface) which brings results of calculations from OTHER (non CatsE) thread pools into
   // an IO (or something that IS managed by CE)
  val asychMolIO : IO[Int] = IO.async_( cb =>  // CE Thread blocks semantically until this cb is invoked by a separate thread
     threadPool.execute { ()=> // computation NOT managed by CE
      val result = computeMeaningOfLifeEither()
      cb(result) // CE is notified w/the result
    }
  )

  // Exercise 1 : Generalize the lifting
   def asyncToIO[A](computation: () => A)(ec: ExecutionContext) : IO[A] = IO.async_( cb  => {
     Try {
       cb(Right(computation()))  // not on different thread pool
     }
   })

  def asyncToIO_daniel[A](computation: () => A)(ec: ExecutionContext) : IO[A] = IO.async_ { cb => {
    ec.execute{() => {
      val result = Try(computation()).toEither
      cb(result)
    }
    }
  }}


  // Exercise 2: Can you lift a future into an IO?
  lazy val molFuture : Future[Int] = Future { computeMeaningOfLife()}

  def futureToIO[A]( fut: => Future[A]): IO[A] = IO.async_ { cb => {
      fut.onComplete { tryResult =>
        val result = tryResult.toEither
        cb (result)
      }
  }}

  // Cats effect already has an effect to convert a future to an IO
  val asynchMolIO_v2 : IO[Int] = IO.fromFuture(IO(molFuture))

  // exercise: Can you create a never ending IO?
  def neverEndingIO : IO[Int] = IO.async_ (  _ => ()) // no callback ==  no finish
  def neverEndingIO_v2 = IO.never
  

  /* THE FULL ASYNC CALL */
  def demoAsyncCancellation() = {
    val asyncMOLIO_v2 : IO[Int] = IO.async{ (cb: Callback[Int]) =>
    /* Finalizer in case computation gets cancelled
      Finalizers are of type IO[Unit]
      not specifying finalizer => Option[IO[Unit]]
      creating option is an effect => IO[Option[IO[Unit]]]
     */
      // return IO[Option[IO[Unit]]]
      IO {
        threadPool.execute { () =>
          val result = computeMeaningOfLifeEither()
          cb(result)
        }
      }.as(Option(IO("Cancelled!").debug.void))
    }

    for {
      fib <- asyncMOLIO_v2.start
      _ <- IO.sleep(500.millis) >> IO("cancelling").debug >> fib.cancel
      _ <- fib.join
    } yield ()
  }




  override def run = //asychMolIO.debug >> IO(threadPool.shutdown())
  //asyncToIO( ()=>computeMeaningOfLife() )(ec).debug.void
  //asyncToIO_daniel(()=>computeMeaningOfLife() )(ec).debug.void
  //futureToIO(molFuture).debug.void
  //asynchMolIO_v2.debug.void
    demoAsyncCancellation().debug.void
}
