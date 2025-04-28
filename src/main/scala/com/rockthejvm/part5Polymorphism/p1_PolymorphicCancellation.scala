package com.rockthejvm.part5Polymorphism

import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.{Applicative, Functor, Monad}
import cats.effect.{IO, IOApp, MonadCancel, Poll}
import com.rockthejvm.utils.general.*

import scala.concurrent.duration.*


object p1_PolymorphicCancellation extends IOApp.Simple {

  trait MyApplicativeError[F[_], E] extends Applicative[F]{
    def raiseError[A](e: E): F[A]
    def handleErrorWith[A](ma: F[A])(func: E => F[A]): F[A]
    //def handleError[A](ma: F[A])(func: E => A): F[A] = handleErrorWith(ma)(e => pure(func(e)))
  }

  trait MyMonadError[M[_], E] extends  MyApplicativeError[M, E] with Monad[M]{
    //def ensure[A](ma: M[A])(error: E)(func: A => Boolean): M[A]
  }

  // MonadCancel
  trait MyPoll[F[_]] {
    def apply[A](fa: F[A]) : F[A]
  }

  trait MyMonadCancel[F[_], E] extends MyMonadError[F, E] {
    def canceled: F[Unit]
    def uncancelable[A](poll: Poll[F] => F[A]) : F[A]
  }

  // monad cancel for IO
  val monadCancelIO: MonadCancel[IO, Throwable] = MonadCancel[IO]

  // we can create values
  val molIO : IO[Int] = monadCancelIO.pure(42)
  val ambitiousMonIO : IO[Int] = monadCancelIO.map(molIO)(_ * 10)


  val mustCompute: IO[Int] = monadCancelIO.uncancelable { _ =>
    for {
      _ <- monadCancelIO.pure("once started, I can't go back....")
      res <- monadCancelIO.pure(52)
    } yield res
  }

  import cats.syntax.flatMap.* // for flatmap below
  import cats.syntax.functor.* // for map ....
  // can generalize code
  def mustComputeGeneral[F[_], E](using mc: MonadCancel[F, E]): F[Int] = mc.uncancelable { _ =>
    for {
      _ <- mc.pure("once started, I can't go back....")
      res <- mc.pure(52)
    } yield res
  }

  val mustCompute_v2 = mustComputeGeneral[IO, Throwable]

    // allow cancellation listeners
  val mustComputeWithListener = mustCompute.onCancel(IO("I'm being cancelled").void)
  val mustComputeWithListener_v2 = monadCancelIO.onCancel(mustCompute, IO("I'm being cancelled").void)
  // onCancel as extension method:
  import cats.effect.syntax.monadCancel.*  // .onCancel extension method is defined for this...

  // allow finalizers
  val aComputationWithFinalizers = monadCancelIO.guaranteeCase(IO(42)) {
    case Succeeded(fa) => fa.flatMap(a => IO(s"Successful: $a")).void
    case Errored(e) => IO.raiseError(Throwable(s"errored: $e"))
    case Canceled() => IO.raiseError(Throwable("canceled!"))
  }

  // bracket pattern is specific to MonadCancel
  val aComputationWithUsage = monadCancelIO.bracket(IO(42)) (value => IO(s"using the meaning of life $value").void)
   { value => IO("releasing the meaning of life").void }

  // bracket pattern is specific to MonadCancel
  val aComputationWithUsage_v2 = monadCancelIO.bracket(IO(42)) ( value =>
    IO(s"Using the meaning of life: $value")
    ){ value =>
    IO("releasing the meaning of life...").void
  }


////////////////////////////////////////////////////////////////////////////////////////////////////


  // EXERCISE: Generalize the following piece of code


  def inputPwd[F[_], E] (using m: MonadCancel[F, E]) = for {
    _ <- m.pure("input pwd").debug
    _ <- m.pure("typing pwd").debug
    _ <- unsafeSleep(5.second).void
    pwd <- m.pure("ezyPasswd")
  } yield pwd


  def verifyPwd[F[_], E](pwd: String)(using m: MonadCancel[F, E]): F[Boolean] =
     for {
      _ <- m.pure("verifying").debug
      _ <- unsafeSleep(2.second)
      itmatches <- m.pure(pwd == "ezyPasswd")
    } yield  itmatches


   // note, Instructor didn't replace the poll function
  def authFlow[F[_], E](using m: MonadCancel[F, E]): F[Unit] = m.uncancelable(
    poll => {
    for {
      pwd <- poll(inputPwd).onCancel(m.pure("Timed out. Please try again later").debug.void) // this is cancellable
      isOK <- verifyPwd(pwd)
      _ <- if (isOK) m.pure("Authentication successful").debug
      else m.pure("Authentication failed").debug
    } yield ()
  })

  val authProgram = for {
    thd <- authFlow[IO, Throwable].start
    _ <- IO.sleep(3.second) >> IO("Auth timeout, attempting cancel").debug >> thd.cancel
    _ <- thd.join
  } yield ()


  override def run : IO[Unit] = authProgram
}
