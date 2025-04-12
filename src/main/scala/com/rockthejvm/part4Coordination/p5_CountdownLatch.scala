package com.rockthejvm.part4Coordination

import cats.syntax.traverse.*
import cats.effect.std.CountDownLatch
import cats.effect.{Deferred, IO, IOApp, Ref, Resource}
import com.rockthejvm.utils.*

import scala.concurrent.duration.*
import cats.syntax.parallel.*

import java.io.{File, FileWriter}
import scala.io.Source
import scala.util.Random

object p5_CountdownLatch extends IOApp.Simple {

  def trigger(latch: CountDownLatch[IO]) : IO[Unit] = for {
    _ <- IO("Starting race shortly").debug >> IO.sleep(2.seconds)
    _ <- IO("5...").debug >> IO.sleep(1.second)
    _ <- latch.release
    _ <- IO("4...").debug >> IO.sleep(1.second)
    _ <- latch.release
    _ <- IO("3...").debug >> IO.sleep(1.second)
    _ <- latch.release
    _ <- IO("2...").debug >> IO.sleep(1.second)
    _ <- latch.release
    _ <- IO("1...").debug >> IO.sleep(1.second)
    _ <- latch.release
    _ <- IO("GO GO GO!").debug
  } yield ()

  def createRunner(id: Int, latch: CountDownLatch[IO]) : IO[Unit] = for {
    _ <- IO(s"[runner $id] waiting for signal...").debug
    _ <- latch.await // block fiber until count = 0
    _ <- IO(s"[runner $id] Running").debug
  } yield ()

  def sprint() : IO[Unit] = for {
    latch <- CountDownLatch[IO](5)
    fAnnouncer <- trigger(latch).start
    _ <- (1 to 10).toList.parTraverse_((id => createRunner(id, latch)))
    _ <- fAnnouncer.join
  } yield ()

  /*
  * Exercise: simulate a file downloader on multiple threads
  * */
  object FileServer {
    val fileChunksList = Array("I love Scala.", "Cats effect seems quite fun.",
      "Never would I have thought I would do low-level concurrency withe pure FP")

    def getNumChunks: IO[Int] = IO(fileChunksList.length)
    def getFileChunk(n: Int): IO[String] = IO(fileChunksList(n-1))
  }

  def writeToFile(path: String, content: String) : IO[Unit] = {
    val fileResource = Resource.make((IO(new FileWriter(new File(path)))))(writer => IO(writer.close()))
    fileResource.use( writer =>  IO(writer.write(content)) )
  }

  def appendFileContents(fromPath: String, toPath: String) : IO[Unit] = {
    val compositeResource = for {
      reader <- Resource.make(IO(Source.fromFile(fromPath))) (source => IO(source.close()))
      writer <- Resource.make(IO(new FileWriter(new File(toPath), true)))(writer => IO(writer.close()))
    } yield (reader, writer)

    compositeResource.use {
      case (reader, writer) => IO(reader.getLines().foreach(writer.write))
    }
  }

  /*
    - call file server API and get the number of chunks (n)
    - start a CDLatch
    - start n fibers which download a chunk of the file
    - block on the latch until each task is finished
    - after all chunks are done, stitch the files together under the same file on disk
   */


  def downloadFile(filename: String, destFolder: String) : IO[Unit] = for {
    n <- FileServer.getNumChunks
    latch <- CDLatch(0) //CountDownLatch[IO](n)
    _ <- (1 to n).toList.parTraverse_(i => IO(s"downloading chunk $i").debug >> IO.sleep(Random.nextInt(1000).millis) >> FileServer.getFileChunk(i).flatMap(ch => writeToFile(s"${destFolder}/ch${i}", ch)) >>
      IO(s"downloaded chunk $i").debug >> latch.release)
    _ <- latch.await
    //_ <- (1 to n).toList.map(i => appendFileContents(s"${destFolder}/ch${i}", s"${destFolder}/${filename}")).sequence
    _ <- (1 to n).toList.traverse(i => appendFileContents(s"${destFolder}/ch${i}", s"${destFolder}/${filename}"))
  } yield ()
  // note:  Daniel used 'traverse' to join the files, whereas I used a map and then sequence.  Both worked

  override def run : IO[Unit] = downloadFile("final.txt","/Users/robertherrick/Development/_tempCats")
}

/*
EXERCISE:  Implement your own Countdown latch with Ref and Deferred
*/

object RJHCDLatch extends IOApp.Simple {

    // the list of running tasks is kept in an array which is mananged with a ref
    def workerProcess(id: Int, sleepSec: Int, ref: Ref[IO, Array[Int]], deferred: Deferred[IO, Int]) : IO[Unit] = for {
      _ <- IO(s"[Process $id] Starting").debug
      _ <- IO.sleep(sleepSec.second)
      _ <- IO(s"[Process $id] complete, releasing").debug
      currentArray <- ref.get
      _ <- if (currentArray.length <= 1) deferred.complete(id) else IO.unit
      _ <- ref.update(arr => arr.tail)
    } yield()

    def runRobsLatch(latchSize: Int) : IO[Unit] = {
      val prox = Ref[IO].of { (1 to latchSize).toArray}
      for {
        ref <- prox
        defer <- Deferred[IO, Int]
        _ <- (1 to latchSize).toList.parTraverse_( id =>  workerProcess(id, Random.nextInt(4), ref, defer)).start
        id <- defer.get
        - <- IO(s"We're done now! we got $id").debug
      } yield ()

    }


    def doIknowDef : IO[Unit] = for {
      defer <- Deferred[IO, Int]
      _ <- IO("wating").debug >> IO.sleep(3.seconds) >> defer.complete(99)
      _ <- defer.get
      _ <- IO("endit").debug
    } yield ()


  def run = //doIknowDef
  runRobsLatch(5)
}

abstract class CDLatch {
  def await : IO[Unit]
  def release: IO[Unit]
}

object CDLatch {
  sealed trait State
  case object Done extends State
  case class Live(remainingCount: Int, signal: Deferred[IO, Unit]) extends State

  def apply(count: Int): IO[CDLatch] = for {
    signal <- Deferred[IO, Unit]
    state <- Ref[IO].of[State](Live(count, signal))
  } yield new CDLatch:
    override def await: IO[Unit] = state.get.flatMap(state => if (state==Done) IO.unit else signal.get)

    override def release: IO[Unit] = state.update {
      case Done => {
        signal.complete(())
        Done
      }
      case Live(1, signal) => {
        signal.complete(())
        Done
      }
      case Live(x, signal) => Live(x - 1, signal)
    }
}

object CDLatchDaniel {
  sealed trait State
  case object Done extends State
  case class Live(remainingCount: Int, signal: Deferred[IO, Unit]) extends State

  def apply(count: Int): IO[CDLatch] = for {
    signal <- Deferred[IO, Unit]
    state <- Ref[IO].of[State](Live(count, signal))
  } yield new CDLatch:
    override def await: IO[Unit] = state.get.flatMap(state => if (state==Done) IO.unit else signal.get)

    override def release: IO[Unit] = state.modify {
      case Done => Done -> IO.unit
      case Live(1, signal) => Done  -> signal.complete(()).void
      case Live(x, signal) => Live(x - 1, signal) -> IO.unit
    }.flatten.uncancelable
}