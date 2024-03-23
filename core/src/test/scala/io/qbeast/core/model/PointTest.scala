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
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable

/**
 * Tests for [[Point]].
 */
class PointTest extends AnyFlatSpec with Matchers {

  "Point" should "have a correct constructor" in {
    val point = Point(immutable.IndexedSeq(1.0, 2.0, 3.0))
    point.dimensionCount shouldBe 3
    point.coordinates(0) shouldBe 1.0
    point.coordinates(1) shouldBe 2.0
    point.coordinates(2) shouldBe 3.0
  }

  it should "support vararg constructor" in {
    val point = Point(1.0, 2.0, 3.0)
    point.dimensionCount shouldBe 3
    point.coordinates(0) shouldBe 1.0
    point.coordinates(1) shouldBe 2.0
    point.coordinates(2) shouldBe 3.0
  }

  it should "implement equals correctly" in {
    val point1 = Point(1.0, 2.0)
    val point2 = Point(1.0, 2.0)
    val point3 = Point(1.0, 2.0, 3.0)
    val point4 = Point(1.0, 3.0)
    val point5 = Point(2.0, 2.0)
    point1 shouldBe point1
    point1 shouldBe point2
    point1 shouldNot be(point3)
    point1 shouldNot be(point4)
    point1 shouldNot be(point5)
  }

  it should "support many factor scale" in {
    val point = Point(1.0, 2.0)
    point.scale(Seq(2.0, 3.0)) shouldBe Point(2.0, 6.0)
  }

  it should "support single factor scale" in {
    val point = Point(1.0, 2.0)
    point.scale(2.0) shouldBe Point(2.0, 4.0)
  }

  it should "support many shift move" in {
    val point = Point(1.0, 2.0)
    point.move(Seq(2.0, 3.0)) shouldBe Point(3.0, 5.0)
  }

  it should "support single shift move" in {
    val point = Point(1.0, 2.0)
    point.move(2.0) shouldBe Point(3.0, 4.0)
  }

  it should "support less" in {
    val point1 = Point(1.0, 2.0)
    val point2 = Point(1.1, 2.1)
    val point3 = Point(1.1, 1.1)
    val point4 = Point(0.9, 2.1)
    point1 < point2 shouldBe true
    point1 < point3 shouldBe false
    point1 < point4 shouldBe false
  }

  it should "support less or equal" in {
    val point1 = Point(1.0, 2.0)
    val point2 = Point(1.0, 2.0)
    val point3 = Point(1.1, 2.1)
    val point4 = Point(0.9, 2.1)
    val point5 = Point(1.1, 1.9)
    point1 <= point2 shouldBe true
    point1 <= point3 shouldBe true
    point1 <= point4 shouldBe false
    point1 <= point5 shouldBe false
  }

}
