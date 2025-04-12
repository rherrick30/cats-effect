package com.rockthejvm.part4Coordination

import cats.effect.std.CyclicBarrier
import cats.effect.{Deferred, IO, IOApp, Ref}
import cats.syntax.parallel.*
import cats.syntax.traverse.*

import scala.util.Random
import scala.concurrent.duration.*
import com.rockthejvm.utils.*

import scala.collection.immutable.Queue

object p6_CyclicBarriers extends IOApp.Simple {

  /*
  A cyclic barrier is a coordination primitive that
  - is initialized with a count
  - has a single API: await

  A cyclic barrier will (semantically) block all fibers calling its await() method until
  we have exactly N fibers waiting, at which point the barrier will unblock all fibers
  and reset to its original state.  Any further fiber will again block until we have
  exactly N fibers waiting.
   */

  // Use case: Signing up for a soon-to-be launched social network
  def createUser( id: Int, barrier: CBRJH): IO[Unit] = for {
    _ <- IO.sleep((Random.nextDouble() * 500).toInt.millis)
    _ <- IO(s"[user $id] Just heard there's a new social network - signing up from the waitlist...").debug
    _ <- IO.sleep((Random.nextDouble() * 1500).toInt.millis)
    _ <- IO(s"[user $id] On the waitlist now").debug
    _ <- barrier.await // blocks the fiber when there are exactly N users waiting
    _ <- IO(s"[user $id] OMG this is so cool").debug
  } yield ()

  def openNetwork(): IO[Unit] = for {
    _ <- IO("[announcer] The Rock the JVM social network is up for registration: Launching when we have 10 users").debug
    barrier <- CBRJH(10)
    _ <- (1 to 14).toList.parTraverse(id => createUser(id, barrier))
  } yield ()

  /*
  Exercise:  Implement your own CB with Ref + Deferred
  * */



  override def run: IO[Unit] = openNetwork()
}

abstract class CBRJH {
  def await : IO[Unit]
}

//object CBRJH_ItWorksButIsNotEfficient {
//  sealed trait State
//  case class Waiting(remainingCount: Int, q: Queue[Deferred[IO, Unit]]) extends State
//
//  def apply(numThreads: Int): IO[CBRJH] = for {
//    state <- Ref[IO].of[Waiting](Waiting(numThreads,Queue()))
//  } yield new CBRJH:
//        override def await: IO[Unit] = for {
//          signal <- Deferred[IO, Unit]
//          _ <- state.modify {
//            case Waiting(a, q) if (a == 1) => Waiting(0, Queue()) -> q.toList.traverse(_.complete(()).void)
//            case Waiting(a, q) => Waiting((a + numThreads - 1) % numThreads, q.enqueue(signal)) -> signal.get
//          }.flatten
//        } yield ()
//}


// My original problem was that I forgot the NEW DEFERRED.  I worked that out with the above commented
// block that did have new deferred, but was not as effcient as it could have been...

object CBRJH {
  case class State(nWaiting: Int, signal: Deferred[IO, Unit])

  def apply(numThreads: Int): IO[CBRJH] = for {
    signal <- Deferred[IO, Unit]
    state <- Ref[IO].of(State(numThreads, signal))
  } yield new CBRJH {
    override def await  = Deferred[IO, Unit].flatMap { newSignal =>
      state.modify {
        case State(1, signal) => State(numThreads, newSignal) -> signal.complete(()).void
        case State(a, signal) => State(a - 1, signal) -> signal.get
      }.flatten
    }
  }
}

