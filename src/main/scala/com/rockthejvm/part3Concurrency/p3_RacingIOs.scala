package com.rockthejvm.part3Concurrency

import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.effect.{Fiber, IO, IOApp, Outcome}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.*
import com.rockthejvm.utils.*

object p3_RacingIOs extends IOApp.Simple {

  def runWithSleep[A](value: A, duration: FiniteDuration) : IO[A] =
    (
      IO(s"starting computation for $value").debug >>
      IO.sleep(duration) >>
      IO(s"computation for $value was done").debug >>
      IO(value)
    ).onCancel(IO(s"computation canceled for $value").debug.void)

  def testRace() = {
    val meaningOfLife = runWithSleep(42, 1.second)
    val favLang = runWithSleep("Scala", 2.seconds)

    val first: IO[Either[Int, String]] = IO.race(meaningOfLife, favLang)

    first.flatMap {
      case Left(a) => IO(s"Left is the winner: $a")
      case Right(b) => IO(s"right gets it this time $b")
    }

  }

  def racePair() = {
    val meaningOfLife = runWithSleep(42, 3.second)
    val favLang = runWithSleep("Scala", 2.seconds)

    // like race, but you get the result (Outcome) of the winner and a Fiber of the loser
    val raceResult: IO[Either[
      (Outcome[IO, Throwable, Int], Fiber[IO, Throwable, String]),
      (Fiber[IO, Throwable, Int], Outcome[IO, Throwable, String])
    ]] = IO.racePair(meaningOfLife, favLang)

    raceResult.flatMap {
      case Left(outMol, fibLang) => fibLang.cancel >> IO(s"Mol won: ${outMol}")
      case Right(fibMol, outLang) => fibMol.join.flatMap( m=> {
        IO(s"Lang won, but we were nice enough to stick around for both $m and ${outLang} at last")
      })
    }
  }

  /*
    EXERCISES:
    1 - implement a timeout pattern with race
    2 - implement method to return a losing effect (unrace)
    3 - write a race function in terms of race pair
  */
  def timeout[A](io: IO[A], duration: FiniteDuration): IO[A] =
    IO.race(io, IO.sleep(duration)).flatMap {
      case Left(a) => IO(a)
      case Right(_) => IO.raiseError(new RuntimeException("Operation timed out"))
    }

  // There is also a first class method for timeout as this is so common...
  val importantTask =  IO.sleep(2.second) >> IO(43).debug
  val testTimeout = timeout(importantTask, 1.second)
  val testTimeout2 = importantTask.timeout(1.seconds)

  def unrace[A, B](ioa: IO[A], iob: IO[B]) : IO[Either[A, B]] =
    IO.racePair(ioa,iob).flatMap {
      case Left(outA, fibB) => fibB.join.flatMap {
        case Succeeded(effect) => effect.flatMap(b=> IO(Right(b)))
        case Errored(e) => IO.raiseError(new RuntimeException("b errored out"))
        case Canceled() => IO.raiseError(new RuntimeException("b was canceled"))
      }
      case Right(fibA, outB) => fibA.join.flatMap {
        case Succeeded(effect) => effect.flatMap(a=> IO(Left(a)))
        case Errored(e) => IO.raiseError(new RuntimeException("a errored out"))
        case Canceled() => IO.raiseError(new RuntimeException("a was canceled"))
      }
    }

  def testUnrace() = {
    val meaningOfLife = runWithSleep(42, 3.second)
    val favLang = runWithSleep("Scala", 2.seconds)
    unrace(meaningOfLife,favLang)
  }

  def simpleRace[A, B] (ioa: IO[A], iob: IO[B]) : IO[Either[A, B]] =
    IO.racePair(ioa, iob).flatMap {
      case Left(outA, fibB) => outA match {
        case Succeeded(fa) => fibB.cancel >> fa.map( a => Left(a))
        case Errored(e) => fibB.cancel >> IO.raiseError(e)
        case Canceled() => fibB.join.flatMap {
          case Succeeded(effect) => effect.flatMap(b=> IO(Right(b)))
          case Errored(e) => IO.raiseError(new RuntimeException("b errored out (a was canceled)"))
          case Canceled() => IO.raiseError(new RuntimeException("a and b were both canceled"))
        }
      }
      case Right(fibA, outB) => outB match {
        case Succeeded(fa) => fibA.cancel >> fa.map( b => Right(b))
        case Errored(e) => fibA.cancel >> IO.raiseError(e)
        case Canceled() => fibA.join.flatMap {
          case Succeeded(effect) => effect.flatMap(a=> IO(Left(a)))
          case Errored(e) => IO.raiseError(new RuntimeException("a errored out (b was canceled)"))
          case Canceled() => IO.raiseError(new RuntimeException("a and b were both canceled"))
        }
      }
    }


        //      case Right(fibA, outB) => fibA.cancel >> outB match
//        case Succeeded(b) => IO(Right(b))
//        case Errored(e) => IO.raiseError(new RuntimeException("b errored out"))
//        case Canceled() => IO.raiseError(new RuntimeException("b was canceled"))
//
//    }

  override def run: IO[Unit] = testUnrace().debug.void
    //timeout(IO.sleep(2.second) >> IO(425).debug, 1.second).void
  //racePair().debug.void

}
