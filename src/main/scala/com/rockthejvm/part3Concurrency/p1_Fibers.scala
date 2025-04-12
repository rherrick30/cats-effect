package com.rockthejvm.part3Concurrency

import cats.effect.kernel.Outcome.*
import cats.effect.{Fiber, IO, IOApp, Outcome}
import com.rockthejvm.utils.*

import scala.concurrent.duration.*

object p1_Fibers extends IOApp.Simple {

  val meaningOfLife = IO.pure(42)
  val faveLanguage = IO.pure("Scala")

  // these will run on the same thread as verified by debug
  def sameThreadIOs() = for {
    _ <- meaningOfLife.debug
    _ <- faveLanguage.debug
  } yield ()

  // introduce the fiber primitive - a description of some computation which
  //  will run on some thread managed by the CATS runtime...and the thread is super lightweight
  // 3 type arguments: Effect type, Error type, Desired Value Type
  def createFiber: Fiber[IO, Throwable, String] = ???

  // it's almost impossible to create Fibers manually like above. Instead, they are created using the API like so:
  // 'start' is what creates the fiber from the IO
  val aFibert: IO[Fiber[IO, Throwable, Int]] = meaningOfLife.debug.start
  // This is actually a fiber wrapped in an IO
  // the fiber is NOT started

  def differentThreadIos(): IO[Unit] = for {
    _ <- aFibert
    _ <- faveLanguage.debug
  } yield ()

  // joining a fiber (joining means waiting for a fiber to finish)
  def runOnSOmeOtherThread[A](io: IO[A]): IO[Outcome[IO, Throwable, A]] = for {
    fib <- io.start
    result <- fib.join // an effect which waits for the fiber to terminate
  } yield result
  /*
    Possible Outcomes:
    - success with an IO
    - failure with an exception
    - cancelled

   */

  val someIOOnAnotherThread = runOnSOmeOtherThread(meaningOfLife)
  val someResultsFromAnotherThread = someIOOnAnotherThread.flatMap {
    case Succeeded(effect) => effect
    case Errored(e)        => IO(0)
    case Canceled()        => IO(0)
  }

  def throwOnAnotherThread() = for {
    fib <- IO.raiseError[Int](new RuntimeException("no number for you")).start
    result <- fib.join
  } yield result

  def testCancel() = {
    val task = IO("starting").debug >> IO.sleep(1.second) >> IO("done").debug
    // on cancel is good for cleaning up resources (file handles, dbconnex, etc) in case of cancellation
    val taskWithCancellationHandler =
      task.onCancel(IO("dude I was totally cancelled!").debug.void)
    for {
      fib <- taskWithCancellationHandler.start // on a separate thread
      _ <- IO.sleep(500.millis) >> IO("cancelling").debug
      _ <- fib.cancel // cancel this fiber
      result <- fib.join
    } yield result
  }

  /** Exercises:
    *   1. Write a function that runs an IO on another thread, and, depending on
    *      the result of the fiber
    *      - return the result in an IO
    *      - if errored or cancelled, return a failed IO
    *
    *   2. Write a function that takes two IOs, runs them on different fibers
    *      and returns an IO with a tuple containing both results.
    *      - if both IOs complete successfully, tuple their results
    *      - if the first IO returns an error, raise that error (ignoring the
    *        second IO's result/error)
    *      - if the first IO doesn't error but second IO returns an error, raise
    *        that error
    *      - if one (or both) canceled, raise a RuntimeException
    *   3. Write a function that adds a timeout to an IO:
    *      - IO runs on a fiber
    *      - if the timeout duration passes, then the fiber is canceled
    *      - the method returns an IO[A] which contains
    *        - the original value if the computation is successful before the
    *          timeout signal
    *        - the exception if the computation is failed before the timeout
    *          signal
    *        - a RuntimeException if it times out (i.e. cancelled by the
    *          timeout)
    */
  import cats.effect.implicits.*
  import cats.effect.unsafe.implicits.global

  // 1
  def processResultsFromFiberMine[A](io: IO[A]): IO[A] = for {
    fib <- io.debug.start
    result <- fib.join // an effect which waits for the fiber to terminate
  } yield result match
    case Succeeded(fa) => fa.unsafeRunSync()
    case Errored(e)    => IO.raiseError(e).unsafeRunSync()
    case Canceled() =>
      IO.raiseError(new RuntimeException("a proper fail")).unsafeRunSync()

  def processResultsFromFiber[A](io: IO[A]): IO[A] = {
    val ioResult = for {
      fib <- io.debug.start
      result <- fib.join // an effect which waits for the fiber to terminate
    } yield result

    ioResult.flatMap {
      case Succeeded(fa) => fa
      case Errored(e)    => IO.raiseError(e)
      case Canceled()    => IO.raiseError(new RuntimeException("cancelled"))

    }
  }

  def testEx1() = {
    val aComputation = IO("Starting1").debug >> IO.sleep(1.second) >> IO(
      "complete1"
    ).debug >> IO(42)
    processResultsFromFiber(aComputation).void
  }

  def testEx1mine() = {
    val aComputation2 = IO("Starting2").debug >> IO.sleep(1.second) >> IO(
      "complete2"
    ).debug >> IO(78)
    processResultsFromFiberMine(aComputation2).void
  }

  // 2
  import cats.syntax.apply.*
  def tupleIOs[A, B](ioa: IO[A], iob: IO[B]): IO[(A, B)] = {

    // RJH Note: the order in the below is important: if you start and join
    // fa BEFORE doing same for fb, it forces completion of a before b whereas
    // if it's like the below fa, and fb are started at the same time and thus
    // total time is less
    val ioTuple = for {
      fibA <- ioa.start
      fibB <- iob.start
      resA <- fibA.join
      resB <- fibB.join
    } yield (resA, resB)

    ioTuple.flatMap {
      case (Succeeded(fa), Succeeded(fb)) => (fa, fb).mapN((a, b) => (a, b))
//      case (Succeeded(fa), Succeeded(fb)) => for {
//        a <- fa
//        b <- fb
//      } yield (a, b)
      case (Errored(e), _) => IO.raiseError(e)
      case (_, Errored(e)) => IO.raiseError(e)
      case (Canceled(), _) | (_, Canceled()) =>
        IO.raiseError(new RuntimeException("a proper cancel"))
    }

  }

  def testEx2() = {
    val FirstIO = IO.sleep(2.seconds) >> IO(1).debug
    val SecondIO = IO.sleep(3.seconds) >> IO(2).debug
    tupleIOs(FirstIO, SecondIO).debug.void
  }

  // 3
  def timeout[A](ioa: IO[A], duration: FiniteDuration): IO[A] = {

    val result = for {
      fib <- ioa.start
      _ <- (IO.sleep(
        duration
      ) >> fib.cancel).start // careful -> fibers can leak!
      // _ <- fib.cancel
      result <- fib.join
    } yield result

    result.flatMap {
      case Succeeded(fa) => fa
      case Errored(e)    => IO.raiseError(e)
      case Canceled() =>
        IO.raiseError(new RuntimeException("cancel of computation"))
    }
  }

  def testEx3() = {
    val aComputation = IO("Starting1").debug >> IO.sleep(2.second) >> IO(
      "complete1"
    ).debug >> IO(42)
    timeout(aComputation, 3.seconds).debug.void
  }

  import cats.syntax.parallel.*
  override def run: IO[Unit] = {
    // sameThreadIOs()
    // differentThreadIos()
    // runOnSOmeOtherThread(meaningOfLife).debug.void // IO(Succeeded(IO(42)))
    // someResultsFromAnotherThread.debug.void
    // throwOnAnotherThread().debug.void // IO(Errored(....))
    // testCancel().debug.void
    // (testEx1(), testEx1mine()).mapN((a,b) => println("dun"))
    // testEx2()
    testEx3()
  }
}
