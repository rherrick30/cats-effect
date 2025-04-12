package com.rockthejvm.part4Coordination

import cats.effect.{IO, IOApp}
import cats.effect.std.Semaphore
import cats.syntax.parallel.*
import scala.concurrent.duration.*
import scala.util.Random
import com.rockthejvm.utils.*

object p4_Semaphores extends IOApp.Simple {

  // A semaphore is a synchronization primitive which allows only a specific number of
  // threads into a critical region.
  val semaphore: IO[Semaphore[IO]] = Semaphore[IO](2) //2 total permits

  // example: limiting the number of concurrent sessions on a server
  def doWorkWhileLoggedIn() : IO[Int] = IO.sleep(1.second) >> IO(Random.nextInt(100))

  def login(id: Int, sem: Semaphore[IO]) : IO[Int] = for {
    _ <- IO(s"[session $id] waiting to log in").debug
    _ <- sem.acquire
    // critical section
    _ <- IO(s"[session $id] logged in").debug
    res <- doWorkWhileLoggedIn()
    _ <- IO(s"[session $id] done: $res, logging out").debug
    // end critical section
    _ <- sem.release
  } yield res

  def demoSemaphore() = for {
    sem <- Semaphore[IO](2)
    fuser1 <- login(1, sem).start
    fuser2 <- login(2, sem).start
    fuser3 <- login(3, sem).start
    _ <- fuser1.join
    _ <- fuser2.join
    _ <- fuser3.join
  } yield ()

  // Request multiple permits
  def weightedLogin(id: Int, requredPermits: Int, sem: Semaphore[IO]) : IO[Int] =  for {
    _ <- IO(s"[session $id] waiting to log in").debug
    _ <- sem.acquireN(requredPermits)
    // critical section
    _ <- IO(s"[session $id] logged in").debug
    res <- doWorkWhileLoggedIn()
    _ <- IO(s"[session $id] done: $res, logging out").debug
    // end critical section
    _ <- sem.releaseN(requredPermits)
  } yield res

  def demoMultiplePermits() = for {
    sem <- Semaphore[IO](10)
    fuser1 <- weightedLogin(1, 2, sem).start
    fuser2 <- weightedLogin(2, 2, sem).start
    fuser3 <- weightedLogin(3, 7, sem).start
    _ <- fuser1.join
    _ <- fuser2.join
    _ <- fuser3.join
  } yield ()

  /*
  Exercise: Find and fix what is wrong with the below

   */
  val mutex = Semaphore[IO](1)
  val users = (1 to 10).toList.parTraverse { id =>  for {
      sem <- mutex
      _ <- IO(s"[session $id] waiting to log in").debug
      _ <- sem.acquire
      // critical section
      _ <- IO(s"[session $id] logged in").debug
      res <- doWorkWhileLoggedIn()
      _ <- IO(s"[session $id] done: $res, logging out").debug
      // end critical section
      _ <- sem.release
    } yield res
  }

    // The problem is that each user got their own semaphore!
  def usersFixed(sem: Semaphore[IO]) = (1 to 10).toList.parTraverse { id =>
    for {
      _ <- IO(s"[session $id] waiting to log in").debug
      _ <- sem.acquire
      // critical section
      _ <- IO(s"[session $id] logged in").debug
      res <- doWorkWhileLoggedIn()
      _ <- IO(s"[session $id] done: $res, logging out").debug
      // end critical section
      _ <- sem.release
    } yield res
  }

  def testUsersFixed = for {
    sem <- mutex
    _ <- usersFixed(sem).debug
  } yield ()

  def usersFixedAnotherWay = mutex.flatMap( sem => {

    (1 to 10).toList.parTraverse { id =>
      for {
        _ <- IO(s"[session $id] waiting to log in").debug
        _ <- sem.acquire
        // critical section
        _ <- IO(s"[session $id] logged in").debug
        res <- doWorkWhileLoggedIn()
        _ <- IO(s"[session $id] done: $res, logging out").debug
        // end critical section
        _ <- sem.release
      } yield res
    }

  })


  override def run = //demoSemaphore()
  //demoMultiplePermits()
  //users.void
  //  testUsersFixed.void
    usersFixedAnotherWay.void
}
