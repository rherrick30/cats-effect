package com.rockthejvm.part4Coordination

import cats.effect.{IO, IOApp, Ref}
import com.rockthejvm.utils.*
import scala.concurrent.duration.*

object p1_Refs extends IOApp.Simple {

  // A ref is a purely functional atomic reference in cats effect
  val atomicMol: IO[Ref[IO, Int]] = Ref[IO].of(42)
  val atomicMol_v2: IO[Ref[IO, Int]] = IO.ref(42)

  // modifying is an effect
  def increaseMol(newInt: Int) : IO[Unit] = atomicMol.flatMap(ref=>
    ref.set(newInt) // thread safe
  )

  // obtain a value
  val getMol : IO[Int] = atomicMol.flatMap(ref=> ref.get) // also thread safe

  val gsMol : IO[Int] = atomicMol.flatMap( ref=> ref.getAndSet(43)) // gets old and sets the new

  // updating w/a function
  val fMol : IO[Unit] = atomicMol.flatMap( ref =>
    ref.update(_ + 3)
  )

  // update and get
  val updatedMol : IO[Int] = atomicMol.flatMap(ref =>
    ref.updateAndGet(_ + 1) // get the new value
    // there is also getAndUpdate to get the old value
  )

  // modifying w/a function and return a different type
  val modifyedMol : IO[String] = atomicMol.flatMap( r =>
    r.modify( v => (v * 10, s"the new value is $v")
  ))

  def demoConcurrentWorkImpure(): IO[Unit] = {
    import cats.syntax.parallel._
    var count = 0

    def task(workload: String) : IO[Unit] = {
      val wordcount = workload.split(" ").length
      for {
        _ <- IO(s"counting words for '$workload' : $wordcount").debug
        newCount <- IO(count + wordcount)
        _ <- IO(s"New total $newCount").debug
        _ <- IO(count += wordcount)
      } yield ()
    }
    List("I love cats effect","This ref thing is useless","Rob writes a lot of code")
      .map(task)
      .parSequence  // changes from List[IO[Unit]] to IO[List[Unit]]
      .void // discards the result so we are back to IO[Unit]

  }
  /*
    The above will print something like this...

  [io-compute-6] counting words for 'I love cats effect' : 4
  [io-compute-4] counting words for 'This ref thing is useless' : 5
  [io-compute-4] New total 5
  [io-compute-6] New total 4
  [io-compute-3] counting words for 'Rob writes a lot of code' : 6
  [io-compute-3] New total 15

   ... clearly showing it is not thread safe by not incrementing the counter faithfully each time

    Drawbacks:
    - hard to read/debug
    - pure and impure code
    - NOT THREAD SAFE
   */

  // Here's a better way!!!
  def demoConcurrentWorkPure(): IO[Unit] =  {
    import cats.syntax.parallel._
    def task(workload: String, total : Ref[IO, Int]) : IO[Unit] = {
      val wordcount = workload.split(" ").length
      for {
        _ <- IO(s"counting words for '$workload' : $wordcount").debug
        newCount <- total.updateAndGet(_ + wordcount)
        _ <- IO(s"New total $newCount").debug
      } yield ()
    }

    val items =  List("I love cats effect", "This ref thing is useless", "Rob writes a lot of code")
    for{
      initialCount <- IO.ref(0)
      _ <- items.map(s=> task(s, initialCount)).parSequence
    }  yield ()

  }

  // Exercise: refactor this function to be thread-safe by using ref
  def tickingClockImpure() : IO[Unit] = {
    import cats.syntax.parallel._
    var ticks: Long = 0L
    def tickingClock: IO[Unit] = for {
      _ <- IO.sleep(1.second)
      _ <- IO(System.currentTimeMillis()).debug
      _ <- IO(ticks += 1)
      _ <- tickingClock
    } yield()

    def printTicks : IO[Unit] = for {
      _ <- IO.sleep(5.seconds)
      _ <- IO(s"TICKS: $ticks").debug
      _ <- printTicks
    } yield ()

    for {
      _ <- (tickingClock, printTicks).parTupled
    } yield ()
  }

  def tickingClockPure() : IO[Unit] = {
    import cats.syntax.parallel._
    val ticks = IO.ref(0L)

    def tickingClock(myTick: Ref[IO, Long]): IO[Unit] = for {
      _ <- IO.sleep(1.second)
      _ <- IO(System.currentTimeMillis()).debug
      _ <- myTick.update(_ + 1 )
      _ <- tickingClock(myTick)
    } yield ()

    def printTicks(tickRef: Ref[IO, Long]) : IO[Unit] = for {
      _ <- IO.sleep(5.seconds)
      curTick <- tickRef.get
      _ <- IO(s"TICKS: ${curTick}").debug
      _ <- printTicks(tickRef)
    } yield ()

    for {
      starterTick <- ticks
      _ <- (tickingClock(starterTick), printTicks(starterTick)).parTupled
    } yield ()
  }

  // NOTE ON ABOVE:  I first tried to do this with a IO[Ref[IO]] declared in the body of 'tickingClockPure'
  // (i.e. WITHOUT passing to sub functions as a parameter) but this didn't work-> the count was never updated.
  // That is because when you do a 'map' of the Ref ( r <- IO[Ref[IO]] ) THAT produces the reference to be used.
  // so if you are calling map from the IO[Ref[IO]] in the for comprehension, you are simply resetting the ref
  // to basis each time you do!



  override def run: IO[Unit] = //demoConcurrentWorkImpure().debug.void
    //demoConcurrentWorkPure().debug.void
   //tickingClockImpure().debug.void
    tickingClockPure().debug.void
}
