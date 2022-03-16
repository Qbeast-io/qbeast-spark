package io.qbeast

object TestClasses {
  case class T1(a: Int, b: String, c: Double)

  case class T2(a: Int, c: Double)

  case class T3(a: Int, b: Double, c: String, d: Float)

  case class TestStrings(a: String, b: String, c: String)
  case class TestDouble(a: Double, b: Double, c: Double)
  case class TestFloat(a: Float, b: Float, c: Float)
  case class TestBigDecimal(a: BigDecimal, b: BigDecimal, c: BigDecimal)
  case class TestInt(a: Int, b: Int, c: Int)
  case class TestLong(a: Long, b: Long, c: Long)
  case class TestNull(a: Option[String], b: Option[Double], c: Option[Int])

  case class IndexData(id: Long, cube: Array[Byte], weight: Double, state: String)

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
