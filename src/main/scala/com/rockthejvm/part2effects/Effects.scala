package com.rockthejvm.part2effects

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import java.time.LocalDateTime


object Effects extends App {

  // pure functional programming
  // substitution
  def combine(a: Int, b: Int) : Int = a + b
  val five = combine(2, 3)
  val five_v2 = 2 + 3
  val five_v3 = 5

  // referential transparency = can replace an expression with its value
  // as many times as we want without changing behavior

  val printSomething : Unit = println("cats effect")
  val printSomething_v2 : Unit = () // not the same as above ~ called "impure function"

  // aother example is changing a variable
  var anInt = 3
  val changeVar : Unit = ( anInt += 1 )

  // side effects are inevitable for useful programs

  // Effect Types
  /*
  Desired properties of effects maps:
  - type signature describes the kind of calculation that will be performed
  - type signature describes the VALUE that will be calculated
  - when side effects are needed, effect construction is separate from effect execution
   */

  // Example: Option
  /*
   - describes a possibly absent value
   - describes the type of A if it exists
   - construction of an Option requires no side effects
  THUS: Option is an Effect type
  * */
  val anOption: Option[Int] = Option(2)

  // Example: Future
  /*
   - describes an asynchronous calculation that will be peformed somethime in the future
   - computes a value of type A it its successful
   - side effects ARE needed (you need a thread on a execution context)
   and execution is not separate from construction
  THUS: Future is NOT an effect type
   */
  given ex: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
  val aFuture : Future[Int] = Future(42)

  // Example MyIO data type
  /*
    - Type signature indicates any computation that MIGHT produce side effects
    - If successful a value of A will be produced if necessary
    - MyIO requires side effect only in the evaluation of () => A and if
    side effects are needed they are NOT produced (triggered) in the creation of MyIO
  THUS:  This is an effect type.  In fact its the most general effect type imaginable!
  */
   case class MyIO[A](unsafeRun: () => A){
    def map[B](f: A => B) : MyIO[B] = MyIO(() => f(unsafeRun()))
    def flatMap[B](f: A => MyIO[B]) : MyIO[B] = MyIO(() => f(unsafeRun()).unsafeRun())
  }

  val myIO : MyIO[Int] = MyIO(()=>{
    println("I am writing something") // not triggered
    42
  })
  myIO.unsafeRun() // NOW...its triggered


  /*
   Exercises
  1. And IO which returns the current time of the system when invoked
  2. An IO which measures the duration of a computation (an IO unsafe run)
  3. Create an IO which prints something to the console
  4. Create an IO with reads from a std input
  */
  // 1
  def clock : MyIO[Long]  = MyIO(() => {
    System.currentTimeMillis()
  })

  //2
  def measureMine[A](computation: MyIO[A]) : MyIO[Long] = MyIO(() => {
    val startTime = clock.unsafeRun()
    computation.unsafeRun()
    val endTime = clock.unsafeRun()
    endTime - startTime
  })

  def measure[A](computation: MyIO[A]) : MyIO[Long] = for {
    ct <- clock
    _ <- computation
    ft <- clock
  } yield  ft - ct

  // 3
  def printSomethang(line: String) : MyIO[Unit]  = MyIO(()=> println(line))

  // 4
  import scala.io.StdIn.readLine
  def readSomething : MyIO[String] = for {
      _ <- printSomethang("whats your name, sugar?")
      herName <- MyIO[String](()=> readLine())
   } yield herName

  println(s"before sleep its\n${System.currentTimeMillis()}")
  val nowIts = clock.unsafeRun()
  println(s"at \n${System.currentTimeMillis()} the time is almost\n${nowIts}")

  def LONG_TASK: MyIO[String] = MyIO(() => {
    Thread.sleep(5000)
    "That was a nice nap"
  })

  val howLongWasIt = measure[String](LONG_TASK)
  println(s"The long task took ${howLongWasIt.unsafeRun()} millis")
  println(s"now its ${clock.unsafeRun()} millis")

  printSomethang("something")
  val derNamen = readSomething.unsafeRun()
  println(s"Hi $derNamen")

  def testConsole : MyIO[Unit] = for {
    s1 <- readSomething
    s2 <- readSomething
    _ <- printSomethang(s"hi ${s1} and ${s2}")
  } yield ()

  testConsole.unsafeRun()

}
