package com.rockthejvm.part5Polymorphism

import cats.effect.{Concurrent, Deferred, Fiber, IO, IOApp, Outcome, Ref, Spawn}
import com.rockthejvm.utils.general.*

import scala.collection.immutable.Queue
import scala.concurrent.duration.*

object p3_PolymorphicCoordination extends IOApp.Simple {

  // CONCURRENT - Ref + Deferred for ANY effect type
  // describes the ability of Cats Effect to create the fundamental
  // concurrency primitives

  trait MyConcurrent[F[_]] extends Spawn[F] {
    def ref[A](a: A) : F[Ref[F, A]]
    def deferred[A] : F[Deferred[F, A]]
  }

  val concurrentIO = Concurrent[IO]  // fetch given instance of ConcurrentIO
  val aDeferred = Deferred[IO, Int] // requires presence of a given Concurrent[IO] in score
  val aDeferred_IO = concurrentIO.deferred[Int] // another way of doing the above
  val aRef = concurrentIO.ref(42)

  // capabilities: pure, map/flatMap, raiseError, uncancelable, start (fibers) + ref/deferred

  import cats.syntax.flatMap.*
  import cats.syntax.functor.*
  import cats.effect.syntax.spawn.*
  import cats.effect.syntax.monadCancel.*

  def polymorphicEggBoiler[F[_]](using concurrent: Concurrent[F]): F[Unit] = {
    def eggReadyNotification(signal: Deferred[F, Unit]) = for {
      _ <- concurrent.pure("Egg boiling on some other fiber, waiting...").debug
      _ <- signal.get
      _ <- concurrent.pure("EGG READY!").debug
    } yield ()

    def tickingClock(counter: Ref[F, Int], signal: Deferred[F, Unit]): F[Unit] = for {
      _ <- unsafeSleep[F, Throwable](1.second)
      count <- counter.updateAndGet(_ + 1)
      _ <- concurrent.pure(count).debug
      _ <- if (count >= 10) signal.complete(()).void else tickingClock(counter, signal)
    } yield ()

    for {
      counter <- concurrent.ref(0)
      signal <- concurrent.deferred[Unit]
      notificationFib <- eggReadyNotification(signal).start
      clock <- tickingClock(counter, signal).start
      _ <- notificationFib.join
      _ <- clock.join
    } yield ()
  }

  /* Exercise
    1. Refactor the racePair example to be generic
    2. Generalize the Mutex
   */
  type RaceResult[F[_], A, B] = Either[
    (Outcome[F, Throwable, A], Fiber[F, Throwable, B]),
    (Fiber[F, Throwable, A], Outcome[F, Throwable, B])
  ]

  type EitherOutcome[F[_], A, B] = Either[Outcome[F, Throwable, A], Outcome[F, Throwable, B]]

  def racePairGeneric[F[_], A, B](fa: F[A], fb: F[B])(using concurrent: Concurrent[F]): F[RaceResult[F, A, B]] =
    concurrent.uncancelable { poll =>
      for {
        signal <- concurrent.deferred[EitherOutcome[F, A, B]]
        fibA <-  concurrent.guaranteeCase(fa)(outcomeA => signal.complete(Left(outcomeA)).void).start
        fibB <- concurrent.guaranteeCase(fb)(outcomeB => signal.complete(Right(outcomeB)).void).start
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
//////////////////// AND HERES THE GENERIC MUTEX

  abstract class GenericMutex[F[_]] {
    def acquire(fibId: Int): F[Unit]
    def release(fibId: Int): F[Unit]
  }

    object GenericMutex {
      type Signal[F[_]] = Deferred[F, Unit]
      case class State[F[_]](locked: Boolean, waiting: Queue[Signal[F]])
      def unlocked[F[_]] = State[F](false, Queue())
//
//      def createSignal(): IO[Signal] = Deferred[IO, Unit]
//
//      def create: IO[Mutex] = Ref[IO].of(unlocked).map {
//        createMutexWithCancellation
//      }
      def createSignal[F[_]](using concurrent: Concurrent[F])(): F[Signal[F]] = concurrent.deferred[Unit]

      def createSimpleMutex[F[_]](state: Ref[F, State[F]])(using concurrent: Concurrent[F]): GenericMutex[F] = new GenericMutex:
        override def acquire(fibId: Int) = createSignal[F]().flatMap { signal =>
          state.modify {
            case State(false, _) => State(locked = true, waiting = Queue()) -> concurrent.pure(())
            case State(true, queue) => State(true, queue.enqueue(signal)) -> signal.get
          }.flatten
        }

        override def release(fibId: Int): F[Unit] = state.modify {
          case State(false, _) => unlocked -> concurrent.pure(())
          case State(true, queue) => if (queue.isEmpty) unlocked -> concurrent.pure(())
          else {
            val (signal, rest) = queue.dequeue
            State(true, rest) -> signal.complete(()).void // note the extra () to send 'unit' to the complete (it is of type unit)
          }
        }.flatten

      def createMutexWithCancellation[F[_]](state: Ref[F, State[F]])(using concurrent: Concurrent[F]): GenericMutex[F] = new GenericMutex[F]:
        override def acquire(fibId: Int) = concurrent.uncancelable(poll => createSignal[F]().flatMap { signal =>

          val cleanup = state.modify {
            case State(locked, queue) =>
              val newQueue = queue.filterNot(_ eq signal)
              State(true, newQueue) -> release(fibId)
          }.flatten

          state.modify {
            case State(false, _) => State(locked = true, waiting = Queue()) -> concurrent.pure(())
            case State(true, queue) => State(true, queue.enqueue(signal)) -> poll(signal.get).onCancel(cleanup)
          }
        }.flatten
        )

        // release is already atomic which makes is cancel-aware
        override def release(fibId: Int): F[Unit] = state.modify {
          case State(false, _) => unlocked -> concurrent.pure(())
          case State(true, queue) => if (queue.isEmpty) unlocked -> concurrent.pure(())
          else {
            val (signal, rest) = queue.dequeue
            State(true, rest) -> signal.complete(()).void // note the extra () to send 'unit' to the complete (it is of type unit)
          }
        }.flatten

    }


  override def run: IO[Unit] = polymorphicEggBoiler[IO]
}
