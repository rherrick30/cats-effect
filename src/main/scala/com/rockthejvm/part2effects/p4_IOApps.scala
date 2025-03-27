package com.rockthejvm.part2effects

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.unsafe.implicits

import scala.io.StdIn

object IOApps {
  val program = for {
    line <- IO(StdIn.readLine())
    _ <- IO(println(s"You've just written ${line}"))
  } yield ()
}

// override the run method to do what you need
object FirstCEApp extends IOApp {
  def run(args: List[String]) : IO[ExitCode] =
    IOApps.program.as(ExitCode.Success) // same as map(_ => success)
}

// This is a simple app that takes no parameters
object FirstSimpleCEApp extends IOApp.Simple {
  def run = IOApps.program // returns IO[Unit]
}