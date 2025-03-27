package com.rockthejvm.part2effects

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object p3_IOErrorHandling extends App {
  given ex: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

  // IO: pure, delay, defer
  // create failed effects
  val aFailedCompute: IO[Int] = IO.delay( throw new RuntimeException("A Failure!"))
  val aFailure : IO[Int] = IO.raiseError(new RuntimeException("a proper fail"))

  //aFailedCompute.unsafeRunSync()
  //aFailure.unsafeRunSync()

  // handle exceptions
  val dealWIthIt = aFailure.handleErrorWith {
    case _ : RuntimeException => IO.delay(println("I am still here"))
  }
  dealWIthIt.unsafeRunSync()

  // turn into an Either
  val effectAsEIther: IO[Either[Throwable, Int]] = aFailure.attempt

  // redeem: transform the failure and the success in one go
  val resultAsString: IO[String] = aFailure.redeem (
    ex => s"Fail: $ex",
    value => s"SUCCESS: $value"
  )
  println(resultAsString.unsafeRunSync())

  // redeemWith
  val resultAsEffect : IO[Unit] = aFailure.redeemWith(
    ex => IO(println(s"IO Fail: $ex")),
    value => IO(println(s"IO SUCCESS: $value"))
  )
  resultAsEffect.unsafeRunSync()

  // EXERCISES

  // 1 - construct potentially failed IOs from standard data types:  Option, Try, Either
  def option2IO[A](option: Option[A])(ifEmpty: Throwable) : IO[A] = option match
    case Some(value) => IO.pure(value)  // DON'T NEED PURE, but it's an option in this case
    case None => IO.raiseError(ifEmpty)

  def tryToIO[A](aTry: Try[A]) : IO[A] = aTry match {
    case Success(value) => IO.pure(value)
    case Failure(err) => IO.raiseError(err)
  }

  def EitherToIo[A](anEither: Either[Throwable ,A]) : IO[A] = anEither match
    case Right(value) => IO.pure(value)
    case Left(err) => IO.raiseError(err)

  // 2 - handle err and handle error with in terms of other Apis
  def handleIOError[A](io: IO[A])(handler: Throwable => A) : IO[A] = io.redeemWith (
    ex => IO.pure(handler(ex)),
    value => IO.pure(value)
  )

  def handleIOErrorInstructor[A](io: IO[A])(handler: Throwable => A): IO[A] =
    io.redeem(handler,identity)


  def handleIOErrorWith[A](io: IO[A])(handler: Throwable => IO[A]) : IO[A] = io.redeemWith (
    ex => handler(ex),
    value => IO.pure(value)
  )

  def handleIOErrorWithInstructor[A](io: IO[A])(handler: Throwable => IO[A]): IO[A] =
    io.redeemWith(handler, IO.pure)

  // NOTE:  handleErrorWith() does the same thing as the above


}
