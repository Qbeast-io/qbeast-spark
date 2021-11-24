package io.qbeast

object TestClasses {
  case class T1(a: Int, b: String, c: Double)

  case class T2(a: Int, c: Double)

  case class Client1(id: Option[Long], name: Option[String], age: Option[Int])

  case class ClientString(id: String, name: String, age: String)

  case class ClientStringOption(id: Option[String], name: Option[String], age: Option[String])

  case class Client2(id: Long, name: String, age: Int)

  case class Client3(id: Long, name: String, age: Int, val2: Long, val3: Double)

  case class Client4(
      id: Long,
      name: String,
      age: Option[Int],
      val2: Option[Long],
      val3: Option[Double])

}
