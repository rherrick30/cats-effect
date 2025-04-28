package com.rockthejvm.utils.general

import cats.Functor
import cats.syntax.functor.*

import scala.concurrent.duration._
import cats.effect.MonadCancel


import java.time.LocalTime

extension [F[_], A](fa: F[A])
  def debug(using functor: Functor[F]): F[A] = fa.map { a=>
    val t = Thread.currentThread().getName
    println(s"{${LocalTime.now()}}: [$t] $a")
    a
  }


def unsafeSleep[F[_], E](duration: Duration)(using mc: MonadCancel[F, E]): F[Unit] =
  mc.pure(Thread.sleep(duration.toMillis)) // NOT semantic blocking