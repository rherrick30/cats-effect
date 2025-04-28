package com.rockthejvm.part5Polymorphism

import cats.effect.kernel.Concurrent
import cats.effect.{IO, IOApp, Temporal}
import com.rockthejvm.utils.general.*

import scala.concurrent.duration.*

object p4_PolymorphicTemporalSuspension extends IOApp.Simple {

  // Temporal - Time blocking effects
  trait MyTemporal[F[_]] extends Concurrent[F] {
    def sleep(
        time: FiniteDuration
    ): F[Unit] // semantically blocks this fiber for a specified time
  }

  // abilities: pure, map/flatMap, raiseError, uncancelable, start, ref/deferred, + sleep
  val temporalIO = Temporal[IO] // given Temporal[IO] in scope
  val chainOfEffects =
    IO("loading").debug *> IO.sleep(1.second) *> IO("game ready").debug
  val chainOfEffects_v2 = temporalIO.pure("loading").debug *> temporalIO.sleep(
    1.second
  ) *> temporalIO.pure("game ready").debug

  /*
  Exercise:  Generalize the following piece of code:
  */
  import cats.syntax.flatMap.*
  
  def timeout[F[_], A](using temporal: Temporal[F])(fa: F[A], duration: FiniteDuration): F[A] = {
    val timeoutEffect = temporal.sleep(duration)
    val result = temporal.race(fa, timeoutEffect)

    result.flatMap {
      case Left(v) => temporal.pure(v)
      case Right(_) => temporal.raiseError(new RuntimeException("Computation timed out."))
    }
  }


  override def run: IO[Unit] = ???
}
