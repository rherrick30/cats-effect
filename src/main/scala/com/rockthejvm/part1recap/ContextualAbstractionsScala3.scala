package com.rockthejvm.part1recap

//import sleazyContAbsImport.*

object ContextualAbstractionsScala3 extends App {

  // given / using combo
  given one: Int = 1
  def increment(using d: Int)(amount: Int) : Int = d + amount
  def twelve = increment(11)
  println(s"twelve is $twelve")

  // a more complex use case
  trait Combiner[A] {
    def combine(x: A, y: A) : A
    def empty: A
  }

  def combineAll[A](values: List[A])(using combiner: Combiner[A] ) : A =
    values.foldLeft(combiner.empty)(combiner.combine)

  given intCombiner: Combiner[Int] with {
    override def empty: Int = 1
    override def combine(x: Int, y: Int): Int = x * y
  }

  val oneToNine = List(1,2,3,4,5,6,7,8,9)
  val factorialNine = combineAll(oneToNine)
  println(s"factorialSmall is $factorialNine")

  // synthesize given instances
  given optionCombiner[T](using combiner: Combiner[T]) : Combiner[Option[T]] with {
    override def empty: Option[T] = Some(combiner.empty)
    override def combine(x: Option[T], y: Option[T]): Option[T] =
      //Option(combiner.combine(x.getOrElse(combiner.empty), y.getOrElse(combiner.empty)))
      for {
        vx <- x
        vy <- y
      } yield combiner.combine(vx, vy)
  }

  // note the cast on the List (the input to the combine function) is necessary to case the Somes
  // also, None wont work in the list because None and Some in any combination = None
  // when using a for.  To handle Nones as defaults, use the commented implementation of
  // combine in the Option combiner (the given)
  val twentyOptions: Option[Int] = combineAll(List[Option[Int]](Some(1),  Some(4), Some(5)))
  println(s"twentyOptions is $twentyOptions")

  // Extension methods
  case class Person(name: String){
    def greet:String = s"Hi there, $name"
  }

  extension ( name: String)
    def greet: String = Person(name).greet

  println(s"say hi to Alice: '${"Alice".greet}'")

  // generic extension
  extension[T](list: List[T])
    def reduceAll(using combiner: Combiner[T]) : T = list.foldLeft(combiner.empty)(combiner.combine)

  val fact9_v2 = oneToNine.reduceAll
  println(s"again factorial 9 is ${fact9_v2}")
}

object TypeClassesScala3 extends App {
  case class Person(name: String, age: Int)

  // part 1 - Type Class Definition
  trait JSONSerializer[T] {
    def toJson(value: T) : String
  }

  // part 2 - type class instances
  given stringSerializer: JSONSerializer[String] with {
    override def toJson(value: String): String = "\"" + value + "\""
  }

  given intSerializer: JSONSerializer[Int] with {
    override def toJson(value: Int): String = value.toString
  }

  given personSerializer: JSONSerializer[Person] with {
    override def toJson(value: Person): String = s"{ name: \"${value.name}\", age: ${value.age} }"
  }

  // Part 3 - User facing Api
  def convertToJson[T](value: T)(using serializer: JSONSerializer[T]) : String = serializer.toJson(value)
  println(s"a person in Json form is ${convertToJson(Person("Mindy", 18))}")

  def convertList2Json[T](list: List[T])(using serializer: JSONSerializer[T]) : String =
    list.map(item=> serializer.toJson(item)).mkString(",")

  // Part 4 - Add extension methods for type you support
  extension [T](value: T)
    def toJson(using serializer: JSONSerializer[T]) : String = serializer.toJson(value)

  // need this extension to do lists..the one above doesn't do it... I think because of the parameter!
  extension [T](list: List[T])
    def toJson(using serializer: JSONSerializer[T]) : String = list.map(item=> serializer.toJson(item)).mkString(",")

  val peeps = List(Person("Alice",21), Person("Bob", 44))
  println(peeps)
  println(peeps.map(_.toJson))
  println(peeps.toJson)
}
