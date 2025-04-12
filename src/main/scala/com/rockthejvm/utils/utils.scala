package com.rockthejvm.utils

import cats.effect.IO
import java.time.LocalTime


extension[A](io: IO[A])
  def debug: IO[A] = for {
    a <- io
    t = Thread.currentThread().getName
    _  = println(s"{${LocalTime.now()}}: [$t] $a")
  } yield a

