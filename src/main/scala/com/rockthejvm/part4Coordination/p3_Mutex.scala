package com.rockthejvm.part4Coordination

import cats.effect.kernel.Outcome.{Succeeded, Errored, Canceled}
import cats.effect.{Deferred, IO, IOApp, Ref}
import cats.syntax.parallel.*

import scala.concurrent.duration.*
import scala.util.Random
import com.rockthejvm.utils.*

import scala.collection.immutable.Queue

abstract class Mutex {
  def acquire(fibId: Int): IO[Unit]
  def release(fibId: Int): IO[Unit]
}

//class RJHMutex extends Mutex {
//  override def acquire: IO[Unit] = for {
//    semap <- Deferred[IO, Int]
//    _ <- IO {
//      release = IO{ semap.complete(1) >> IO("release of semaphore").debug.void}.void
//    }
//    _ <- IO("acquired semaphore").debug
//  }  yield ()
//  var release: IO[Unit] = null
//}


object MutexConcurrencyProblems {
  type Signal = Deferred[IO, Unit]
  case class State(locked: Boolean, waiting: Queue[Signal])

  val currentState = State(false, Queue[Signal]())
  val stateRef = Ref[IO].of(currentState)

  def create: IO[Mutex] = Ref[IO].of(currentState).map { state =>
    new Mutex:
      override def acquire(fibId: Int): IO[Unit] = for {
        _ <- IO(s"ACQUIRE (${fibId})....attempting").debug.void
        isLocked <- state.get

        _ <- if (!isLocked.locked)  {
          IO(s"ACQUIRE (${fibId}).....Q was unlocked").debug.void >>
            state.set(State(true, Queue[Signal]())) >>
            IO(s"Queue locked for ${fibId}").debug
        }
        else for {
          witer <- Deferred[IO, Unit]
            _ <- state.set(State(true, isLocked.waiting ++ Queue(witer)))
            qLen <- state.get.map(_.waiting.length)
            _ <- IO(s"ACQUIRE (${fibId})....enqueued (len=${qLen}").debug.void
            _ <- witer.get
            _ <- IO(s"ACQUIRE (${fibId}).....got witer").debug.void
          } yield ()

      } yield ()

      override def release(fibId: Int): IO[Unit] = for {
        currentSt <- state.get
        _ <- if (currentSt.locked) for {
          _ <- if(currentSt.waiting.isEmpty) state.set(State(false, Queue[Signal]())) else for {
            lastEntry <- IO(currentSt.waiting.dequeue)
            _ <- IO(s"RELEASE (${fibId})....popped (len=${lastEntry._2.length.toString})").debug.void
            _ <- lastEntry._1.complete(true)
            _ <- state.set(State(true, lastEntry._2))
          } yield()

        } yield () else IO.unit
      } yield()
  }
}

object RobsMutexWhichStillDoesntWork {
  type Signal = Deferred[IO, Unit]

  case class State(locked: Boolean, waiting: Queue[Signal])

  val currentState = State(false, Queue[Signal]())
  val stateRef = Ref[IO].of(currentState)



  def create: IO[Mutex] = Ref[IO].of(currentState).map { state =>
    new Mutex:
      override def acquire(fibId: Int): IO[Unit] = for {
        _ <- IO(s"ACQUIRE (${fibId})....attempting").debug.void
        witer <- Deferred[IO, Unit]
        _ <- state.modify( isLocked => {
            if (!isLocked.locked) {
              println(s"mutex was unlocked for ${fibId}")
              (State(true, Queue[Signal]()), IO.unit)
            } else
              println(s"mutex was locked for ${fibId}")
              (State(true, isLocked.waiting ++ Queue(witer)), IO.unit)
          }) >> state.get.map( s => if(s.locked) {println(s"we are waiting for ${fibId}")
          witer.get} else IO.unit)
      } yield ()

      override def release(fibId: Int): IO[Unit] = for {
        _ <- IO(s"RELEASE (${fibId}) was called").debug
        currentSt <- state.get
        _ <- if (currentSt.locked) for {
          _ <- if (currentSt.waiting.isEmpty) state.set(State(false, Queue[Signal]())) else for {
            lastEntry <- IO(currentSt.waiting.dequeue)
            _ <- IO(s"RELEASE (${fibId})....popped (len=${lastEntry._2.length.toString})").debug.void
            _ <- lastEntry._1.complete(true)
            _ <- state.set(State(true, lastEntry._2))
          } yield ()

        } yield () else IO.unit
      } yield ()
  }
}

object Mutex {
  type Signal = Deferred[IO, Unit]
  case class State(locked: Boolean, waiting: Queue[Signal])
  val unlocked = State(false, Queue[Signal]())

  def createSignal(): IO[Signal] = Deferred[IO, Unit]

  def create: IO[Mutex] = Ref[IO].of(unlocked).map { createMutexWithCancellation }

  def createSimpleMutex(state: Ref[IO, State]) : Mutex =  new Mutex:
    override def acquire(fibId: Int) = createSignal().flatMap { signal =>
      state.modify {
        case State(false, _) => State(locked = true, waiting = Queue()) -> IO.unit
        case State( true, queue) => State(true, queue.enqueue(signal)) -> signal.get
      }.flatten  // our 'B' for modify was an IO so we use 'flatten' to convert IO[IO[_]]  to IO[_]
    }

    override def release(fibId: Int): IO[Unit] = state.modify {
      case State(false, _) => unlocked -> IO.unit
      case State(true, queue) => if(queue.isEmpty) unlocked -> IO.unit
      else {
        val(signal, rest) = queue.dequeue
        State(true, rest) -> signal.complete(()).void    // note the extra () to send 'unit' to the complete (it is of type unit)
      }
    }.flatten

  def createMutexWithCancellation(state : Ref[IO, State]) : Mutex =  new Mutex:
    override def acquire(fibId: Int) = IO.uncancelable( poll => createSignal().flatMap { signal =>

        val cleanup = state.modify {
          case State(locked, queue) =>
            val newQueue = queue.filterNot(_ eq signal)
            State(true, newQueue) -> release(fibId)
        }.flatten

        state.modify {
          case State(false, _) => State(locked = true, waiting = Queue()) -> IO.unit
          case State( true, queue) => State(true, queue.enqueue(signal)) -> poll(signal.get).onCancel(cleanup)
        }  // our 'B' for modify was itself an IO so we use 'flatten' to convert IO[IO[_]]  to IO[_]
      }.flatten
    )

    // release is already atomic which makes is cancel-aware
    override def release(fibId: Int): IO[Unit] = state.modify {
      case State(false, _) => unlocked -> IO.unit
      case State(true, queue) => if(queue.isEmpty) unlocked -> IO.unit
      else {
        val(signal, rest) = queue.dequeue
        State(true, rest) -> signal.complete(()).void    // note the extra () to send 'unit' to the complete (it is of type unit)
      }
    }.flatten

}

/*
ROB'S TAKEAWAYS ON 'CREATESIMPLEMUTEX' METHOD ABOVE:
1) Use 'flatten' to get rid of a rogue IO -> if you want to read the value.  In this case we went from IO[IO[Unit]] to IO[Unit]
2) Pattern matching is your friend
3) Arrow notation
*/




object p3_Mutex extends IOApp.Simple {

  def criticalTask() : IO[Int] = IO.sleep(1.second) >> IO(Random.nextInt(100))

  def createNonLockingTask(id: Int) : IO[Int] = for {
    _ <- IO(s"task $id working...").debug
    res <- criticalTask()
    _ <- IO(s"task $id got result: $res").debug
  } yield res

  def demoNonLockingTasks(): IO[List[Int]] = (1 to 10).toList.parTraverse(id => createNonLockingTask(id))

  def createLockingTask(id: Int, mutex: Mutex) : IO[Int] = for {
    _ <- IO(s"task $id waiting for permission...").debug
    _ <- mutex.acquire(id)
    // critical section start
    _ <- IO(s"task $id working...").debug
    res <- criticalTask()
    _ <- IO(s"task $id got result: $res").debug
    // critical section end
    _ <- mutex.release(id)
    _ <- IO(s"task $id releasing mutex.").debug
  } yield res

  def demoLockingTasks(): IO[List[Int]] = for {
    mutex <- Mutex.create
    results <- (1 to 10).toList.parTraverse(id => createLockingTask(id, mutex))
  } yield results


  def createCancellingTask(id: Int, mutex: Mutex) : IO[Int] = {
    if(id % 2 == 0) createLockingTask(id, mutex)
    else for {
      fib <- createLockingTask(id, mutex).onCancel(IO(s"[task $id] received cancellation").debug.void).start
      _ <- IO.sleep(2.second) >> fib.cancel
      out <- fib.join
      result <- out match {
        case Succeeded(fa) => fa
        case Errored(_) => IO(-1)
        case Canceled() => IO(-2)
      }
    } yield result
  }

  def demoCancellingTasks() = for {
    mutex <- Mutex.create
    results <- (1 to 10).toList.parTraverse(id => createCancellingTask(id, mutex))
  } yield results

  override def run: IO[Unit] = //demoNonLockingTasks().debug.void
  //demoLockingTasks().debug.void
    demoCancellingTasks().debug.void
}