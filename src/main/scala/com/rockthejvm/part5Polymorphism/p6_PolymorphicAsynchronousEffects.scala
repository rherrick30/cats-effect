package com.rockthejvm.part5Polymorphism

import cats.effect.{Async, Concurrent, IO, IOApp, Sync, Temporal}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import com.rockthejvm.utils.*

case object p6_PolymorphicAsynchronousEffects extends IOApp.Simple {

  // Async: asynchronous computations, "suspended" in F
  trait MyAsync[F[_]] extends Sync[F] with Temporal[F] {
    def executionContext: F[ExecutionContext]
    def async[A](cb : (Either[Throwable, A] => Unit) => F[Option[F[Unit]]]) : F[A]
    def async_[A](cb: (Either[Throwable, A] => Unit) => Unit): F[A]
    def evalOn[A](fa: F[A], ec: ExecutionContext) : F[A]
    def never[A] : F[A]
  }

  val asyncIO = Async[IO]

  // pure, map/flatMap, raiseError, uncancelable, start, ref/deferred, sleep, delay/defer/blocking +
  val ec = asyncIO.executionContext  // gives you the execution context upon which all effects will be executed

  // power: async_ + async (Foreign Function Interface)

  val threadPool = Executors.newFixedThreadPool(10)
  type Callback[A] = Either[Throwable, A] => Unit
  val asyncMeaningOfLife : IO[Int] = IO.async_ { (cb: Callback[Int]) =>
    // start computation on some other thread pool not mananged by Cats Effect
    threadPool.execute { () =>
      println(s"[${Thread.currentThread().getName}] Computing an async MOL")
      cb(Right(42)) // Result flows back to CE as an IO[Int]  (the right turns to Int)
    }
  }

  val asyncMOLComplex : IO[Int] = IO.async { (cb: Callback[Int]) =>
    IO {
      threadPool.execute { () =>
        println(s"[${Thread.currentThread().getName}] Computing an async MOL")
        cb(Right(42))
      }
    }    .as(Some(IO("Cancelled!").debug.void))  // Finalizer in case of cancellation
  }

  //NOTE either of the two above could be done w/the implicit
  //asyncIO.async { ???} or asyncIO.async_ {???}  where ??? is a call back


  // Async also allows for the running of effects on contexts even though they're not managed by CE
  val myExecutionContext = ExecutionContext.fromExecutorService(threadPool)
  val asyncMeaningOfLife_v2 = asyncIO.evalOn(IO(42).debug, myExecutionContext).guarantee(IO(threadPool.shutdown()))

  // Never is a perpetual effect
  val neveIo = asyncIO.never

  /**
   * 1. Implement never and async_ in terms of the big async
   * 2. Tuple two effects with different requirements
   */

  trait MyAsyncModded[F[_]] extends Sync[F] with Temporal[F] {
    def executionContext: F[ExecutionContext]
    def async[A](cb : (Either[Throwable, A] => Unit) => F[Option[F[Unit]]]) : F[A]
    def evalOn[A](fa: F[A], ec: ExecutionContext) : F[A]

    def async_[A](cb: (Either[Throwable, A] => Unit) => Unit): F[A] =
      async(kb => map(pure(cb(kb)))(_ => None))
    def never[A] : F[A] = async_(_ => ()) // because the callback is never called, it never stops
  }

  // 2
    def firstEffect[F[_] : Concurrent, A](a: A) : F[A] = Concurrent[F].pure(a)
    def secondEffect[F[_]: Sync, A](a: A) : F[A] = Sync[F].pure(a)

  import cats.syntax.flatMap.*
  import cats.syntax.functor.*
    def tupledEffect[F[_] : Async, A](a: A): F[(A, A)] = for {
      f1 <- firstEffect(a)
      f2 <- secondEffect(a)
    } yield (f1, f2)

  override def run: IO[Unit] = ???
}
