package com.rockthejvm.part4Coordination

import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.syntax.traverse.*
import cats.effect.{Deferred, Fiber, IO, IOApp, Outcome, Ref}
import com.rockthejvm.utils.*

import scala.concurrent.duration.*

object p2_Defers extends IOApp.Simple {

  // deferred is a primitive for waiting for an effect, while some other effect completes with a value

  val aDeferred: IO[Deferred[IO, Int]] = Deferred[IO, Int]
  val aDeferred2 : IO[Deferred[IO, Int]] = IO.deferred[Int]

  // there are 2 methods:
  // get blocks the calling fiber (semantically) until some other fiber completes the Deferred with a value
  val reader : IO[Int] = aDeferred.flatMap { deferred =>
    deferred.get // will block this fiber till completed, then will get it!
  }

  // complete will release a value to a 'listening' fiber (like  above)
  val writer : IO[Boolean] = aDeferred.flatMap{ signal => signal.complete(42) }

  // they work together like this
  def demoDeferred() : IO[Unit] = {
    def consumer(signal: Deferred[IO, Int]) = for {
      _ <- IO("[consumer] waiting for result").debug
      meaningOfLife <- signal.get
      _ <- IO(s"[consumer] gotz my result: $meaningOfLife").debug
    } yield()

    def producer(signal : Deferred[IO, Int]) = for {
      _ <- IO("[producer] crunching numbers...").debug >> IO.sleep(1.second)
      mol <- IO.pure(42)
      _ <- IO("[producer] crunching complete").debug
      result <- signal.complete(42)
    } yield ()

    for {
      signal <- Deferred[IO, Int]
      fibConsumer <- consumer(signal).start
      fibProducer <- producer(signal).start
      _ <- fibConsumer.join
      _ <- fibProducer.join
    } yield ()
  }

  // simulate downloading some content
  val fileParts = List("I ", "love S", "cala", " with Cat", "s Effect!<EOF>")

  def fileNotifierWithRef() : IO[Unit] = {
    def downloadFile(contentRef: Ref[IO, String]): IO[Unit] =
      fileParts.map{ chunk => IO(s"got '$chunk'").debug >>
        IO.sleep(600.millis) >>
        contentRef.update(currentContent => currentContent + chunk)
      }.sequence // to change List[IO[Unit]] to IO[List[Unit]]
        .void

    def notifyFileComplete(contentRef: Ref[IO, String])  : IO[Unit] = for {
      file <- contentRef.get
      _ <- if(file.endsWith("<EOF>")) IO("File download complete").debug else IO("downloading...").debug >>
         IO.sleep(200.millis) >> notifyFileComplete(contentRef)
    } yield ()

    for {
      content <- Ref[IO].of("")
      fibDownloader <- downloadFile(content).start
      notifier <- notifyFileComplete(content).start
      _ <- fibDownloader.join
      _ <- notifier.join
    } yield ()
  }

  // The above is an instance of "busy waiting": It IS thread safe, but hardly the most efficient approach
  // deferred can save the day here...check it out:
  def fileNotifierWithDeferred() : IO[Unit] = {
    def notifyFileComplete( signal : Deferred[IO, String]) : IO[Unit] = for {
      _ <- IO("[notifier] downloading...").debug
      _ <- signal.get // blocks until file download completes
      _ <- IO("[notifier] File download complete").debug
    } yield ()

    def downloadFile(chunk: String, signal : Deferred[IO, String], contentRef: Ref[IO, String]): IO[Unit] = for {
      _ <- IO(s"got '$chunk'").debug
      _ <- IO.sleep(600.millis)
      updatedValue <- contentRef.updateAndGet(currentContent => currentContent + chunk)
      _ <- if(updatedValue.endsWith("<EOF>")) signal.complete(updatedValue) else IO.unit
    } yield ()

    for {
      content <- Ref[IO].of("")
      sig <- Deferred[IO, String]
      fibNotify <- notifyFileComplete(sig).start
      fibFileTasks <- fileParts.map(part => downloadFile(part, sig, content)).sequence.start
      _ <- fibNotify.join
      _ <- fibFileTasks.join
    } yield ()
  }

  /**
   *  Exercises:
   *  - (medium) write a small alarm notification with two simultaneous IOs
   *    - one that increments a counter every second (a clock)
   *    - one that waits for the counter to become 10, then prints a message "time's up!"
   *
   *  - (mega hard) implement racePair with Deferred.
   *    - use a Deferred which can hold an Either[outcome for ioa, outcome for iob]
   *    - start two fibers, one for each IO
   *    - on completion (with any status), each IO needs to complete that Deferred
   *      (hint: use a finalizer from the Resources lesson)
   *      (hint2: use a guarantee call to make sure the fibers complete the Deferred)
   *    - what do you do in case of cancellation (the hardest part)?
   */
  
  def alarmNotification() : IO[Unit] = {
    def readClock( signal: Deferred[IO, Int]) : IO[Unit] = for {
      _ <- IO("[readClock] Waiting for 10").debug
      x <- signal.get
      _ <- IO(s"[readClock] Time's up, we've reached $x").debug
    } yield ()
      
    def ticker( signal: Deferred[IO, Int], cnt: Ref[IO, Int]) : IO[Unit] = for {
      _ <- IO.sleep(1.second) 
      c <- cnt.updateAndGet(x => x + 1)
      _ <- IO(s"...$c").debug
      _ <- if(c >= 10) signal.complete(c) else ticker(signal, cnt)
    } yield ()
      
    for {
      ref <- Ref[IO].of(0)
      sig <- Deferred[IO, Int]
      fibConsumer <- readClock(sig).start
      fibProducer <- ticker(sig, ref).start
      _ <- fibConsumer.join
      _ <- fibProducer.join
    } yield ()
  }

  // Doesn't account for dual cancellation
  def racePair() : IO[Unit] = {
    
    def sleeper (secs: Int, name: String ) : IO[String] = for {
      _ <- IO(s"Waiting for '${name}'").debug
      _ <- IO.sleep(secs.seconds)
    } yield name

    def finishHandler( secs: Int, name: String , isRight: Boolean, sig : Deferred[IO, Either[String, String]]) : IO[String] =
      sleeper(secs, name).guaranteeCase {
        case Succeeded(fa) => fa.flatMap(result => if (isRight) sig.complete(Right(result)).void else sig.complete(Left(result)).void)
        case Errored(e) => IO.raiseError(e)
        case Canceled() => IO(s"fiber $name was cancelled").debug.void
      }

    for {
      sig <- Deferred[IO, Either[String, String]]
      fibA <- finishHandler(5,"alpha",true,sig).start
      fibB <- finishHandler(2, "beta", false, sig).start
      _ <- IO.sleep(1.second) >> fibB.cancel >> fibA.cancel
      result <- sig.get
      _ <- IO(s"The winner is ${result}").debug
    } yield ()
  }

  type RaceResult[A, B] = Either[
    (Outcome[IO, Throwable, A], Fiber[IO, Throwable, B]),
    (Fiber[IO, Throwable, A], Outcome[IO, Throwable, B])
  ]

  def racePairTake2[A, B](ioa: IO[A], iob: IO[B]) : IO[RaceResult[A, B]] = {

    val cancelCount = Ref[IO].of(0)

    for {
      cc <- cancelCount
      sig <- Deferred[IO, RaceResult[A, B]]
      fibA <- ioa.start
      fibB <- iob.start
      _ <- fibA.join.guaranteeCase {
        case Succeeded(fa) => IO("A won").debug >> fa.flatMap(res => sig.complete(Left(res,fibB))).void
        case Errored(e) => IO("A errored").debug >> sig.complete(Left(Errored(e),fibB)).void
        case Canceled() => IO(s"fiber A was cancelled").debug >> cc.update(i => if(i>=1) {
          sig.complete(Left(Canceled(),fibB))
          i + 1
        } else i + 1).void
      }
      _ <- fibB.join.guaranteeCase {
        case Succeeded(fa) => IO("B won").debug >> fa.flatMap(res => sig.complete(Right(fibA, res))).void
        case Errored(e) => IO("B errored").debug >> sig.complete(Right(fibA, Errored(e))).void
        case Canceled() => IO(s"fiber B was cancelled").debug  >> cc.update(i => if(i>=1) {
          sig.complete(Left(Canceled(),fibB))
          i + 1
        } else i + 1).void
      }



      finalResult <- sig.get
    } yield finalResult
  }


  // HERE IS DANIEL'S TAKE ON IT:
  // The differences with mine are
  // #1:  Adding a cancel handler to do the cancellation
  // #2:  Creating the 'EitherOutcome' so the final RaceResult can be composed during the yield.
  //      This is better because we avoid having to start the thread THEN monitor...
  type EitherOutcome[A, B] = Either[Outcome[IO, Throwable, A], Outcome[IO, Throwable, B]]

  def racePairDaniel[A, B](ioa: IO[A], iob: IO[B]): IO[RaceResult[A, B]] = IO.uncancelable { poll =>
    for {
      signal <- Deferred[IO, EitherOutcome[A, B]]
      fibA <- ioa.guaranteeCase(outcomeA => signal.complete(Left(outcomeA)).void).start
      fibB <- iob.guaranteeCase(outcomeB => signal.complete(Right(outcomeB)).void).start
      result <- poll(signal.get).onCancel { // blocking call - should be cancelable
        for {
          cancelFibA <- fibA.cancel.start
          cancelFibB <- fibB.cancel.start
          _ <- cancelFibA.join
          _ <- cancelFibB.join
        } yield ()
      }
    } yield result match {
      case Left(outcomeA) => Left(outcomeA, fibB)
      case Right(outcomeB) => Right(fibA, outcomeB)
    }
  }


  override def run : IO[Unit] =
    //demoDeferred()
    //fileNotifierWithRef()
    //fileNotifierWithDeferred()
    //alarmNotification()
    racePair()

}
