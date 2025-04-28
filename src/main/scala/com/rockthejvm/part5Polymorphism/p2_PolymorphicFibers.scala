package com.rockthejvm.part5Polymorphism

import cats.effect.{Fiber, FiberIO, IO, IOApp, MonadCancel, Outcome, Spawn}
import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import scala.concurrent.duration.*
import com.rockthejvm.utils.*


object p2_PolymorphicFibers extends IOApp.Simple {

  // Spawn is a type class which generalizes the concept of a fiber
  trait MySpawn[F[_]] extends MonadCancel[F, Throwable] {
    def start[A](fa: F[A]): F[Fiber[F, Throwable, A]] // creates a fiber
    def never[A]: F[A] // forever suspending effect
    def cede : F[Unit] // a "yield" effect: a hint for the Cats runtime to switch the thread
  }

  // GenSpawn is a more generic version of Spawn: Error type is genericised
  trait MyGenSpawn[F[_], E] extends MonadCancel[F, E] {
    def start[A](fa: F[A]): F[Fiber[F, E, A]]
    def never[A]: F[A]
    def cede : F[Unit]
    // TODO
  }

  trait MySpawnActually[F[_]] extends MyGenSpawn[F, Throwable]

  val mol = IO(42)
  val fMol: IO[FiberIO[Int]] = mol.start
  val fMol2: IO[Fiber[IO, Throwable, Int]] = mol.start

  // since Spawn is a MonadCancel, you also get: pure, map/flatMap, uncancelable, raiseError

  val spawnIO = Spawn[IO] // fetched the given/implicit Spawn[IO]

  def ioOnSomeThread[A](io: IO[A]) = for {
    fb <- spawnIO.start(io)   //  use the implicit to start it! assumes presence of a Spawn[IO]
    result <- fb.join
  } yield result


  // generalize the above
  import cats.effect.syntax.spawn.* // start extension methods
  import cats.syntax.functor.* //map
  import cats.syntax.flatMap.*
  def effectOnSomeThread[F[_], A]( fa: F[A] )( using spawn: Spawn[F]) : F[Outcome[F, Throwable, A]]  = for {
    fib <- fa.start // spawn.start(fa) replaced with imported start method from cats.effect.syntax.spawn.*
    result <- fib.join
  } yield result

  val molOnFiber = ioOnSomeThread(mol)
  val molOnFiber_v2 = effectOnSomeThread[IO, Int](mol)

  // Exercise - generalize the following code
  trait MyGenSpawn_v2[F[_], E] extends MonadCancel[F, E] {
    def start[A](fa: F[A]): F[Fiber[F, E, A]]
    def never[A]: F[A]
    def cede: F[Unit]
    // TODO
    def racePair[A, B](fa: F[A])(fb: F[B]) : F[Either[
      (Outcome[F, E, A], Fiber[F, E, B]),
      (Fiber[F, E, A], Outcome[F, E, B] )
    ]]
  }
  // NOT THE ABOVE...that's an example class.  Do the below:
  def generalSimpleRaceWithArrows[F[_], A, B](fa: F[A], fb: F[B])(using spawn: Spawn[F]): F[Either[A, B]] =
    spawn.racePair(fa, fb).flatMap {
      case Left(outA, fibB) => outA match {
        case Succeeded(effectA) => fibB.cancel >> effectA.map(a => Left(a))
        case Errored(e) => fibB.cancel >> spawn.raiseError(e)
        case Canceled() => fibB.join.flatMap {
          case Succeeded(effect) => effect.flatMap(b => spawn.pure(Right(b)))
          case Errored(e) => spawn.raiseError(new RuntimeException("b errored out (a was canceled)"))
          case Canceled() => spawn.raiseError(new RuntimeException("a and b were both canceled"))
        }
      }
      case Right(fibA, outB) => outB match {
        case Succeeded(effectB) => fibA.cancel >>  effectB.map(b => Right(b))
        case Errored(e) => fibA.cancel >> spawn.raiseError(e)
        case Canceled() => fibA.join.flatMap {
          case Succeeded(effect) => effect.flatMap(a => spawn.pure(Left(a)))
          case Errored(e) => spawn.raiseError(new RuntimeException("a errored out (b was canceled)"))
          case Canceled() => spawn.raiseError(new RuntimeException("a and b were both canceled"))
        }
      }
    }


  def generalSimpleRace[F[_], A, B](fa: F[A], fb: F[B])(using spawn: Spawn[F]): F[Either[A, B]] =
    spawn.racePair(fa, fb).flatMap {
      case Left(outA, fibB) => outA match {
        case Succeeded(effectA) => fibB.cancel.flatMap( _ => effectA.map(a => Left(a)))
        case Errored(e) => fibB.cancel.flatMap(_ => spawn.raiseError(e))
        case Canceled() => fibB.join.flatMap {
          case Succeeded(effect) => effect.flatMap(b => spawn.pure(Right(b)))
          case Errored(e) => spawn.raiseError(new RuntimeException("b errored out (a was canceled)"))
          case Canceled() => spawn.raiseError(new RuntimeException("a and b were both canceled"))
        }
      }
      case Right(fibA, outB) => outB match {
        case Succeeded(effectB) => fibA.cancel.flatMap(_ => effectB.map(b => Right(b)))
        case Errored(e) => fibA.cancel.flatMap(_ => spawn.raiseError(e))
        case Canceled() => fibA.join.flatMap {
          case Succeeded(effect) => effect.flatMap(a => spawn.pure(Left(a)))
          case Errored(e) => spawn.raiseError(new RuntimeException("a errored out (b was canceled)"))
          case Canceled() => spawn.raiseError(new RuntimeException("a and b were both canceled"))
        }
      }
    }

  // on the above, I missed that the >> operator is not available generally, that's IO only
  // I found that it worked decently with our tests, but a different effect type may blow up!  

  val fast = IO.sleep(1.second) >> IO(42).debug
  val slow = IO.sleep(2.seconds) >> IO("scala").debug
  val race = generalSimpleRaceWithArrows(fast, slow)


  override def run: IO[Unit] = race.void
}
