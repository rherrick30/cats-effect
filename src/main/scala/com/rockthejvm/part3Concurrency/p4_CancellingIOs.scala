package com.rockthejvm.part3Concurrency

import cats.effect.{IO, IOApp}
import scala.concurrent.duration.*
import com.rockthejvm.utils.*

object p4_CancellingIOs extends IOApp.Simple {

  /*
  *  Ways of calling IOs:
  *  - fib.cancel
  *  - IO.race & other APIs
  *  - manual cancellation
  * */


  val chainofIOs: IO[Int] = IO("waiting").debug >> IO.canceled >> IO(42).debug

  // uncancelable:  A wrapper around an IO which prevents it from being cancelled
  // example: online store, payment processor
  // payment process must not be cancelled
  val specialPaymentSystem =  (
    IO("Payment Running, don't cancel me...").debug >>
    IO.sleep(1.second) >>
    IO("Payment completed.").debug
    ).onCancel(IO("MEGA CANCEL OF DOOM").debug.void)

  val cancellationOfDoom = for {
    fib <- specialPaymentSystem.start
    _ <- IO.sleep(400.millis) >> fib.cancel
    _ <- fib.join
  } yield ()

  val atomicPayment = IO.uncancelable(_ => specialPaymentSystem) // masking
  val atomicPayment_v2 = specialPaymentSystem.uncancelable // equivalent to above

  val cancelationOfDoom_v2 = for {
    fib <- atomicPayment_v2.start
    _ <- IO.sleep(600.millis) >> IO("attempting cancel").debug >> fib.cancel
    _ <- fib.join
  } yield ()

  // The uncancellable wrapper takes a parameter that is a "polling" function.  This function allows
  // you to mark sections within the masked (wrapped) IO as being cancellable.
  // This function is of the form Pool[IO] => IO

  // Example: Toy authentication service.
  // inputing a password van be cancelled (to avoid blocking indefinitely on user input
  // verifying the pwd CANNOT be canceled once started.

  val inputPwd : IO[String] = IO("input pwd").debug >>
    IO("typing pwd").debug >>
    IO.sleep(5.second) >>
    IO("ezyPasswd")

  val verifyPwd = (pwd: String) => IO("verifying").debug >>
    IO.sleep(2.second) >>
    IO(pwd == "ezyPasswd")

  val authFlow : IO[Unit] = IO.uncancelable(poll => {
    for {
      pwd <- poll(inputPwd).onCancel(IO("Timed out. Please try again later").debug.void) // this is cancellable
      isOK <- verifyPwd(pwd)
      _ <- if (isOK) IO("Authentication successful").debug
            else IO("Authentication failed").debug
    } yield ()
  })

  val authProgram = for {
    thd <- authFlow.start
    _ <- IO.sleep(3.second) >> IO("Auth timeout, attempting cancel").debug >> thd.cancel
    _ <- thd.join
  } yield ()

  /*
  Exercises
  */
  // 1
  val cancelBeforeMol = IO.canceled >> IO(42).debug.void
  // prints nothing
  val uncancelableMol = cancelBeforeMol.uncancelable
  // prints 42 because ALL cancel calls are ignored, including a thread own cancel calls

  // 2
  val invinciibleAuthProgram = for {
    thd <- IO.uncancelable(_ => authFlow).start
    _ <- IO.sleep(9.second) >> IO("Auth timeout, attempting cancel").debug >> thd.cancel
    _ <- thd.join
  } yield ()
  // Does NOT time out because wrapping w/cancel kills all nested 'polls' also

  // 3
  val threeStepProgram : IO[Unit] = {
    val sequence = IO.uncancelable(poll =>
      poll(IO("cancelable").debug >> IO.sleep(1.second) >> IO("cancelable end").debug) >>
        IO("uncancelable").debug >> IO.sleep(1.second) >> IO("uncancelable end").debug >>
        poll(IO("second cancelable").debug >> IO.sleep(1.second) >> IO("second cancelable end").debug)
    )

    // prints cancelable & uncancelable (nothing else) as the chain is snapped after
    // step 2.  The cancel was called during step 2, BUT it was not respected until the last
    // step (because 2 is not cancelable, but 3 is due to teh 'poll' statement.  IF there
    // were a step 4 which was NOT cancelable, it would NOT be run as the whole chain
    // would have been canceled in step 3.  So the FIRST cancelable step after receiving
    // the cancel signal will snap the chain


    for {
      fib <- sequence.start
      _ <- IO.sleep(1500.millis) >> IO("CANCELING").debug >> fib.cancel
      _ <- fib.join
    } yield ()
  }

  override def run: IO[Unit] = //cancellationOfDoom
    //cancelationOfDoom_v2
    //authFlow
    //authProgram
    //uncancelableMol
    //invinciibleAuthProgram
    threeStepProgram
}
