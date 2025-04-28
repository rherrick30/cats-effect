package com.rockthejvm.part5Polymorphism

import cats.Defer
import cats.effect.{IO, IOApp, MonadCancel, Sync}

import scala.concurrent.duration.*
import com.rockthejvm.utils.*

import java.io.{BufferedReader, InputStreamReader}
import scala.io.StdIn

object p5_PolymorphicSynchronousEffects extends IOApp.Simple {

  val aDelayedIO = IO.delay { // "suspend" computations in IO
    println("I am an effect") // This does NOT actually evaluate
    24
  }

  val aBlockingIO = IO.blocking { // executed on some specific thread pool for blocking computations
    println("loading...")
    Thread.sleep(1000)
    42
  }

  // The above embodies the concept of "synchronous computation".  The ability to run some computation outside the context of
  // Cats effect and wrap them into Cats effect via an IO....and also the ability to wrap a blocking computation in an IO

  trait MySync[F[_]] extends MonadCancel[F, Throwable] with Defer[F] {
    def delay[A](thunk: => A) : F[A]  // suspension of a computation - will run on the CE thread pool
    def blocking[A](thunk: => A) : F[A] // runs on the blocking thread pool

    def defer[A](thunk: => F[A]) : F[A] = flatten(delay(thunk))
    def defer_v2[A](thunk: => F[A]) : F[A] = flatMap(delay(thunk))(identity)

  }

  val syncIO = Sync[IO] // given Sync[IO] in scope

  // abilities: pure, map/flatmap, raiseError, uncancellable, + delay and blocking
  val aDelayedIO_v2 = syncIO.delay {
    println("I am an effect") // This does NOT actually evaluate
    24
  }

  val aBlockingIO_v2 = syncIO.blocking { // executed on some specific thread pool for blocking computations
    println("loading...")
    Thread.sleep(1000)
    42
  }

  // THESE above are the same as v1, but were created via the implicit!

  val aDeferredIO = IO.defer(aDelayedIO) // takes an effect as a thunk, not a value

  /*
   Exercise: Write a polymorphic console
  */

  trait Console[F[_]] {
    def println[A](a: A): F[Unit]
    def readLine(): F[String]
  }

  // RJH:  I did this before Daniel said he wanted F[Console[F]] not Console[F]
  object Console {
    def createConsole[F[_]](using s: Sync[F]) : Console[F] = new Console[F] {
      def println[A](a: A): F[Unit] = s.delay(println(a))
      def readLine(): F[String] = s.delay[String](StdIn.readLine())
    }
  }

  import cats.syntax.functor.*

  object DanielConsole {
    def createConsole[F[_]](using s: Sync[F]) : F[Console[F]] = s.pure((System.in, System.out)).map {
      case (in, out) => new Console[F]:
        def println[A](a: A): F[Unit] = s.blocking(out.println(a))
        def readLine(): F[String] = {
          val bufferedReader = new BufferedReader(new InputStreamReader(in))
          s.blocking(bufferedReader.readLine())
        }
    }
  }


  val testit = for {
    c <- DanielConsole.createConsole[IO]
    _ <- c.println("whatis yer name?")
    myName <- c.readLine()
  } yield ()


  override def run: IO[Unit] = testit
}
