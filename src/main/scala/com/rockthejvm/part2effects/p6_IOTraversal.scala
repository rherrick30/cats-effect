package com.rockthejvm.part2effects

import cats.effect.IOApp
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
import cats.effect.IO
import com.rockthejvm.utils.*

object p6_IOTraversal extends IOApp.Simple {
  given ec : ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

  def heavyComputation(string: String) : Future[Int] = Future {
    Thread.sleep(Random.nextInt(1000))
    string.split(" ").length
  }

  val workload: List[String] = List("I quite like CE", "Scala is great", "looking forward to some awesome stuff")
  def clunkyFutures() : Unit = {
    val futures: List[Future[Int]] = workload.map(heavyComputation)
    // If i want to wait for all of the above and get a single result,
    // it would be difficult to obtain.  This is where "traverse" comes in
    futures.foreach(_.foreach(println))
  }

  import cats.instances.list.*
  import cats.Traverse
  val listTraverse = Traverse[List]
  private def traverseFutures(): Unit = {
    val singleFuture: Future[List[Int]] = listTraverse.traverse(workload)(heavyComputation)
    singleFuture.foreach(println)
  }


  def computeAsIO(string: String)  : IO[Int] = IO {
    Thread.sleep(Random.nextInt(1000))
    string.split(" ").length
  }.debug

  // several IOs - painful!
  val ios : List[IO[Int]] = workload.map(computeAsIO)
  // one IO with multiple tasks...better!
  val singleIO: IO[List[Int]] = listTraverse.traverse(workload)(computeAsIO)


  // parallel traversal
  import cats.syntax.parallel.*
  val parallelSingleIO : IO[List[Int]] = workload.parTraverse(computeAsIO)


  // Exercises
  def sequence[A](listOfIOs: List[IO[A]]) : IO[List[A]] = listTraverse.traverse(listOfIOs)(_.map(identity))

  def sequenceGeneral[F[_], A](listOfIOs: F[IO[A]])(using traversioso: Traverse[F]): IO[F[A]] =
    traversioso.traverse(listOfIOs)(_.map(identity))

  def sequenceGeneral_v2[F[_] : Traverse, A](listOfIOs: F[IO[A]]): IO[F[A]] =
      Traverse[F].traverse(listOfIOs)(_.map(identity))

  def parSequence[A](listOfIOs: List[IO[A]]) : IO[List[A]] = listOfIOs.parTraverse(identity)

  def parSequenceGeneral[F[_] : Traverse, A](listOfIOs: F[IO[A]]): IO[F[A]] =
    listOfIOs.parTraverse(identity)

  // There is already a wrapper for the above, though...
  val singleIO_v2 : IO[List[Int]] = listTraverse.sequence(ios) // like using our "sequence[A] as defined above"
  val parallelSingleIO_v2 : IO[List[Int]] = ios.parSequence // list using our "parSequence[A]"


  override def run =
    singleIO.map(_.sum).debug.void >> IO.println("now parallel") >> parallelSingleIO.map(_.sum).debug.void
}
