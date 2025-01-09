package com.rockthejvm.part1recap

object CatsTypeClasses extends App {
  /*
  - applicative
  - functor
  - flatmap
  - monad
  - apply
  - applicativeError/monadError
  - traverse
   */

  // functor (mappable data structure)
  import cats.Functor
  import cats.instances.list.*
  trait MyFunctor[F[_]] {
    def map[A, B](initialValue: F[A])(f: A=>B) : F[B]
  }

  val listFunctor = Functor[List]
  def increment[F[_]](container: F[Int])(using functor: Functor[F]) : F[Int] =
    functor.map(container)(_ + 1)

  import cats.syntax.functor.*
  def increment_v2[F[_] : Functor](container: F[Int]): F[Int] = container.map(_ + 1)


  // applicative - ability to "wrap" types
  trait MyApplicative[F[_]] extends MyFunctor [F]{
    def pure[A](value: A): F[A]
  }
  import cats.Applicative
  val applicativeList = Applicative[List]
  val aSimpleList : List[Int] = applicativeList.pure(2)
  println(s"aSimpleList is ${aSimpleList}")
  import cats.syntax.applicative.*
  val anotherSimpleList = 44.pure
  println(s"anotherSimpleList is $anotherSimpleList")

  // flatmap - chaining calculation
  trait MyFlatMap[F[_]] extends MyFunctor[F] {
    def flatMap[A, B](container: F[A])(f: A => F[B]): F[B]
  }
  import cats.FlatMap
  val fmList = FlatMap[List]
  import cats.syntax.flatMap.*
  def crossProduct[F[_] : FlatMap, A, B](fa: F[A], fb: F[B]) : F[(A, B)] =
    fa.flatMap(a=> fb.map(b=> (a, b)))

  //  monad - applicative + flatmap
  trait MyMonad[F[_]] extends MyApplicative[F] with MyFlatMap[F]{
    override def map[A, B](container: F[A])(f: A => B): F[B] =
      flatMap(container)(a => pure(f(a)))
  }
  import cats.Monad
  val monadList = Monad[List]
  // no implicits necessary as the only ones there are are imported w/ FlatMap and Applucative
  def crossProduct_v2[F[_]: Monad, A, B](fa: F[A], fb: F[B]) = for {
    a <- fa
    b <- fb
  } yield (a, b)

  // Applicative Error
  trait MyApplicativeError[F[_], E] extends MyApplicative[F] {
    def raiseError[A](e:E): F[A]
  }

  import cats.ApplicativeError
  type ErrorOr[A] = Either[String, A]
  val applicativeEither = ApplicativeError[ErrorOr, String]
  val desirableValue = applicativeEither.pure(42)
  val failedValue = applicativeEither.raiseError("Oops something failed")

  import cats.syntax.applicativeError.*
  val oops = "Oh, Snap!".raiseError
  println(oops)

  // Monad Error
  trait MyMonadError[F[_], E] extends MyApplicativeError[F, E] with Monad[F]
  import cats.MonadError
  val monadErrorEither = MonadError[ErrorOr, String]

  // Traverse: turned nested classes inside out
  trait MyTraverse[F[_]] extends MyFunctor[F]{
    def traverse[G[_], A, B](container: F[A])(f: A => G[B]) : G[F[B]]
  }

  val listOptions : List[Option[Int]] = List(Some(1), Some(2), Some(3))

  import cats.Traverse
  val listTraverse = Traverse[List]
  val optionList: Option[List[Int]] = listTraverse.traverse(List(1,2,3))(x=> Option(x))
  println(s"optionList is ${optionList}")

  import cats.syntax.traverse.*
  val optionList_v2 : Option[List[Int]] = List(1,2,3).traverse(x => Option(x))
  

}
