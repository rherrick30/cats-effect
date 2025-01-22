package com.rockthejvm.part2effects

import cats.effect.IO

import java.sql.Date
import scala.io.StdIn

object IOIntroduction extends App {

  // IO
  val ourFirstIO: IO[Int] = IO.pure(42) // arg should not have side effects
  val aDelayedIO: IO[Int] = IO.delay {
    println("I'm giving you an integer")
    42
  } // delay can produce side effects...when executed

  val badPure : IO[Int] = IO.pure({
    println("I dont want to see this, but I will because I am using side effects in pure!")
    -99
  })

  // you can also use apply to create IO
  val aDelayedIO_v2 : IO[Int] = IO {
    println("Apply yourself")
    99
  }

  // unsafe run sync is the way to run synchronously
  import cats.effect.unsafe.implicits.global
  println(badPure.unsafeRunSync()) // you will not see side effects again on this bad pure

  val improvedMeaningOfLife = ourFirstIO.map(_ * 2)
  val printedMeaningOfLife = ourFirstIO.flatMap(mol => IO.delay((println(mol))))
  printedMeaningOfLife.unsafeRunSync()

  def smallProgram : IO[Unit] = for {
    line1 <- IO(StdIn.readLine())
    line2 <- IO(StdIn.readLine())
    _ <- IO.delay(println(s"$line1 $line2"))
  } yield ()
  //smallProgram.unsafeRunSync()

  // map - combine IO effects as tuples
  import cats.syntax.apply._
  val combinedMeaningofLife : IO[Int] = (ourFirstIO, improvedMeaningOfLife, badPure).mapN(_ + _ * _ )

  def smallProgram2 : IO[Unit] = (IO(StdIn.readLine()), IO(StdIn.readLine())).mapN(_ + _).map(println)
  //smallProgram2.unsafeRunSync()

  // EXERCISES

  // 1 - Sequence 2 IOs and take the result of the last one
  def sequenceTakeLast[A, B](ioa: IO[A], iob: IO[B]) : IO[B] = for {
    _ <- ioa
    b <- iob
  } yield b

  def sequenceTakeLast_instructor[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    ioa.flatMap(_ => iob)

    // *> is the andThen operator
  def sequenceTakeLast_v2[A, B](ioa: IO[A], iob: IO[B]): IO[B] = ioa *> iob
  // >> andThen with by-name call
  def sequenceTakeLast_v3[A, B](ioa: IO[A], iob: IO[B]) : IO[B] = ioa >> iob


    // 2 - same as #1 but take first io instead
  def sequenceTakeFirst[A, B](ioa: IO[A], iob: IO[B]) : IO[A] = for {
    a <- ioa
    _ <- iob
  } yield a

  // <* is the "andThen and take first" operator
  def sequenceTakeFirst_v2[A, B](ioa: IO[A], iob: IO[B]): IO[A] = ioa <* iob

  // 3 - repeat an IO effect forever
  def forever[A](input: IO[A]) : IO[A] =
    input.flatMap( _ => input)

  def foreverInstructor[A](io: IO[A]) : IO[A] =
    io.flatMap( _ => foreverInstructor(io))

  def forever_2[A](io: IO[A]) : IO[A] =
    io >> forever_2(io)

  println("is it forever?")
  //forever_2(IO(println("forever!"))).unsafeRunSync()

  // the *> causes eager evaluation of io....so this will cause a stack overflow
  // whereas >> which is lazy evaluation, uses case classes w/tail recursion
  def forever_3[A](io: IO[A]): IO[A] =
    io *> forever_3(io)

  //forever_3(IO(println("forever!"))).unsafeRunSync()

  def forever_4[A](io: IO[A]): IO[A] =
    io.foreverM // like v2 (>> using tail recursion

  // 4 - convert an IO to a different type
  def convert[A, B](io: IO[A], value: B) : IO[B] = for {
    a <- io
  } yield value

  def convert_v2[A, B](ioa: IO[A], value: B) =
    ioa.as(value)


  // 5 - discard value in IO and return Unit instead
  def tossIt[A](aio: IO[A]) : IO[Unit] = for {
    _ <- aio
  } yield ()

  def tossit_v2[A] (aio: IO[A]) : IO[Unit] =
    aio.void  /// better than aio.as(()) due to unreadability

  // 6 - fix stack recursion
  def sum(n: Int) : Int =
    if(n<=0) 0 else n + sum(n - 1)

  def sumIO(n: Int) : IO[Int] =
    if (n<=0)
      IO.pure(0)
    else
      (IO.pure(n),IO(sumIO(n - 1)).flatten).mapN(_ + _)

  def sumIOInstructor(n: Int) : IO[Int] =
    if(n<=0) IO(0)
    else for {
      enn <- IO.pure(n)
      lower <- sumIOInstructor(n - 1)
    } yield enn + lower

  // the version with mapN will cause stack overflow...I think because we need the flat mapping for tail recursion
  // note I surrounded mine with IO().flatMap(x=>x) and that will work
  val BOUND = 20000
  val mine =  sumIO(BOUND).unsafeRunSync()
  val his = sumIOInstructor(BOUND).unsafeRunSync()
  println(s"I got ${mine} and Daniel got ${his}...${if (mine==his) "great!" else "phooey"}")

  // 7 - write a fibonacci function which does not crash on recursion
  def fibonacci(n: Int) : IO[BigInt] =
    if (n<= 0) IO.pure(0)
    else if (n== 1) IO.pure(1)
    else for {
      nMinus2 <- fibonacci(n-2)
      nMinus1 <- fibonacci(n - 1)
    } yield nMinus1 + nMinus2

  def fibonacciInstructor(n: Int): IO[BigInt] =
    if (n < 2) IO.pure(1)
    else for {
      nMinus2 <- IO.defer(fibonacci(n - 2)) // same as .delay(...).flatten
      nMinus1 <- IO.delay(fibonacci(n - 1)).flatten
    } yield nMinus1 + nMinus2

  val beforeTime = System.currentTimeMillis()
  println(fibonacci(10).unsafeRunSync())
 (1 to 100).foreach(i => println(s"AT ${(System.currentTimeMillis() - beforeTime) / 1000.0} secs:: the fibonacci of $i is ${fibonacci(i).unsafeRunSync()}"))


}
