package com.rockthejvm.part3Concurrency

import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}
import cats.effect.{IO, IOApp, Resource}

import scala.concurrent.duration.*
import com.rockthejvm.utils.*

import java.io.{File, FileReader}
import java.util.Scanner

object p2_Resources extends IOApp.Simple {

  // use-case: manage a connection lifecycle
  class Connection( url: String) {
    def open: IO[String] = IO(s"opening connection to $url").debug
    def close : IO[String] = IO(s"closing connection to $url").debug
  }

  // Problem:  This leaks resources...the connection is never closed!
  val asynchFetchUrl : IO[Unit] = for {
    fib <- (new Connection("http://mysite.com")).open *> IO.sleep((Int.MaxValue).seconds).start
    _ <- IO.sleep(1.second) *> fib.cancel
  } yield ()

  val correctAsynchFetchUrl: IO[Unit] = for {
    conn <- IO(new Connection("http://mysite.com"))
    fib <- (conn.open *> IO.sleep((Int.MaxValue).seconds)).onCancel(conn.close.void).start
    _ <- IO.sleep(1.second) *> fib.cancel
  } yield ()

  // BRACKET PATTERN
  val bracketFetchUrl = IO(new Connection("http://mysite.com"))
    .bracket(conn => conn.open *> IO.sleep((Int.MaxValue.seconds)))(conn=> conn.close.void)

  val bracketProgram = for {
    fib <- bracketFetchUrl.start
    _ <- IO.sleep(1.second) *> fib.cancel
  } yield ()

  val robsResultfulBracket  = IO(new Connection("http://mysite.com"))
    .bracket { conn =>
      conn.open *>
        IO.pure(3) *>
        IO.sleep(2.seconds) *>
        IO.pure(18)
    } (conn=> conn.close.void)

  /* Exercise
  - open a scanner
  - read all the lines of the file
   */
  def openFileScanner(path: String) : IO[Scanner] =
    IO(new Scanner(new FileReader(new File(path))))

  // This is incorrect as it only read one line (the first obviously)
  def bracketReadFile(path: String) : IO[Unit] = openFileScanner(path)
    .bracket { sc =>
      for {
        line <- IO(sc.nextLine()).debug
        _ <- IO.sleep(100.millis)
      } yield ()
    } (sc => IO(sc.close()))

  def readLineByLine(sc: Scanner): IO[Unit] = if (sc.hasNextLine) IO(sc.nextLine()).debug >> IO.sleep(100.millis) *> readLineByLine(sc)
  else IO.unit

  def bracketReadFileInstructor(path: String) : IO[Unit] =
    IO("opening file at $path").debug *>
      openFileScanner(path).bracket { sc =>
        readLineByLine(sc)
      } ( sc => IO(s"closing $path").debug  >> IO(sc.close()))

  /**
    RESOURCES
  */
  def connFromConfig(path: String) : IO[Unit] =
    openFileScanner(path)
      .bracket{ scanner =>
        // acquire a connection based on the file
        IO(new Connection(scanner.nextLine())).bracket { conn =>
          conn.open.debug >> IO.never
        } (conn => conn.close.debug.void)
      } (scanner => IO("closing file").debug >> IO(scanner.close()))
  // nesting resources are tedious

  // Resources (CatsIO) improve on the bracket pattern by allowing the user to
  // declare the acquisition and disposal of the resource up front, and then
  // use it later (contrasted with nesting as is the case w/ bracket
  val connectionResource: Resource[IO, Connection] =
    Resource.make(IO(new Connection("rockthejvm.com")))(connection => connection.close.void)

  // ... and then, MUCH later :-)
  val resourceFetchUrl = for {
      fib <- connectionResource.use(conn => conn.open >> IO.never).start
      _ <- IO.sleep(1.second) >> fib.cancel
    } yield ()


  // resources are equivalent to brackets
  val simpleResource = IO("some resource")
  val usingResource : String => IO[String] = string => IO(s"using the string $string").debug
  val releaseResource : String => IO[Unit] = string => IO(s"finalizing the string $string").debug.void

  val usingResourceWithBracket = simpleResource.bracket(usingResource)(releaseResource)
  val usingResourceWithResource = Resource.make(simpleResource)(releaseResource).use(usingResource)

  // Exercise : read a file with resource, w/1 line every 100 millis
  def getScanner(path: String) : Resource[IO, Scanner] =
    Resource.make(IO(new Scanner(new FileReader(new File(path)))))(sc => (IO(sc.close()) *>  IO(s"closing scanner for file $path").debug).void)

  def resourceReadFile(path: String): IO[Unit] =
    IO(s"opening file at $path").debug *>
      getScanner(path).use( sc => readLineByLine(sc)) *> IO(s"closed scanner for file $path").debug.void


  def cancelReadFile(path: String) = for {
    fib <- resourceReadFile(path).start
    _ <- IO.sleep(2.second) *> fib.cancel
  } yield ()

  // nested resources : use flatMap to chain them...type is driven by innermost resource
  def connectionFromConfigurationResource(path: String): Resource[IO, Connection] =
    Resource.make(IO(s"opening scanner for file $path").debug >> openFileScanner(path))(sc => (IO(s"closing scanner for file $path").debug *> IO(sc.close())))
      .flatMap(scanner => Resource.make(IO(new Connection(scanner.nextLine())))(conn => conn.close.void))

  val openConnection = connectionFromConfigurationResource("src/main/resources/connex.txt")
    .use(conn => conn.open >> IO.never)

  val openAndCloseWRes : IO[Unit] = for {
    fib <- openConnection.start
    _ <- IO.sleep(1.second) *> fib.cancel
  } yield ()

  def connFromConfResourceClean(path: String): Resource[IO, Connection] = for {
    scanner <- Resource.make(IO(s"opening scanner for file $path").debug >> openFileScanner(path))(sc => (IO(s"closing scanner for file $path").debug *> IO(sc.close())))
    conn <- Resource.make(IO(new Connection(scanner.nextLine())))(conn => conn.close.void)
  } yield conn

  // RJH NOTE: HERE'S how to use BOTH resources in the 'use'
  type resourceBundle = (Connection, Scanner)
  def connFromConfResourceDual(path: String): Resource[IO, resourceBundle] = for {
    scanner <- Resource.make(IO(s"opening scanner for file $path").debug >> openFileScanner(path))(sc => (IO(s"closing scanner for file $path").debug *> IO(sc.close())))
    conn <- Resource.make(IO(new Connection(scanner.nextLine())))(conn => conn.close.void)
  } yield ( conn, scanner)

  val rjhDualResAttempt = connFromConfResourceDual("src/main/resources/connex.txt")
    .use(bundle => bundle._1.open >> IO.println(bundle._2.nextLine()) >> IO.never)

  /// end RJH Aside :-)


  // finalizers can be attached to regular IOs
  val ioWIthFinalizer = IO("some resource").debug.guarantee(IO("freeing resource").debug.void)

  val ioWIthFinalizer_v2 = IO("Some resource").debug.guaranteeCase {
    case Succeeded(fa) => fa.flatMap(result => IO(s"releasing resource $result").debug.void)
    case Errored(e) => IO.raiseError(e)
    case Canceled() => IO.raiseError(new Throwable("The resource was cancelled"))
  }

  override def run = {
    //asynchFetchUrl.void
    //correctAsynchFetchUrl.void
    //bracketProgram.void
    //robsResultfulBracket.map(println)
    //bracketReadFileInstructor("/Users/robertherrick/Downloads/check.sh")
    //resourceFetchUrl.void
    //resourceReadFile("/Users/robertherrick/Downloads/check.sh")
    //cancelReadFile("/Users/robertherrick/Downloads/check.sh")
    //openAndCloseWRes.void
    //ioWIthFinalizer.void
    rjhDualResAttempt.void
  }
}
