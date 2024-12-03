/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast

object TestClasses {
  case class T1(a: Long, b: String, c: Double)

  case class T2(a: Long, c: Double)

  case class T3(a: Long, b: Double, c: String, d: Float)

  case class TestStrings(a: String, b: String, c: String)
  case class TestDouble(a: Double, b: Double, c: Double)
  case class TestFloat(a: Float, b: Float, c: Float)
  case class TestBigDecimal(a: BigDecimal, b: BigDecimal, c: BigDecimal)
  case class TestInt(a: Int, b: Int, c: Int)
  case class TestLong(a: Long, b: Long, c: Long)
  case class TestNull(a: Option[String], b: Option[Double], c: Option[Long])

  case class IndexData(id: Long, cube: Array[Byte], weight: Double)

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

  case class Student(id: Int, name: String, age: Int)

}
