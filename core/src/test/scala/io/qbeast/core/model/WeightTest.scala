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

/**
 * Tests for [[Weight]].
 */
class WeightTest extends AnyFlatSpec with Matchers {
  "Weight" should "compute fraction correctly" in {
    Weight.MinValue.fraction shouldBe 0.0
    Weight.MaxValue.fraction shouldBe 1.0
    Weight(1).fraction shouldBe (1.0 - Int.MinValue) / (Int.MaxValue.toDouble - Int.MinValue)
    Weight(1) shouldBe Weight(Weight(1).fraction)
  }

  it should "implement addition correctly" in {
    Weight(-1) + Weight(-2) shouldBe Weight(-(Int.MinValue + 3))
    Weight.MaxValue + Weight(1) shouldBe Weight.MaxValue
  }

  it should "implement subtraction correctly" in {
    Weight(2) - Weight(1) shouldBe Weight(Int.MinValue + 1)
    Weight.MinValue - Weight(1) shouldBe Weight.MinValue
  }

  it should "implement multiplication correctly" in {
    Weight(1) * Weight(2) shouldBe Weight(Weight(1).fraction * Weight(2).fraction)
    Weight.MinValue * Weight(1) shouldBe Weight.MinValue
    Weight.MaxValue * Weight(1) shouldBe Weight(1)
  }

  it should "implement division correctly" in {
    Weight(1) / Weight(2) shouldBe Weight(Weight(1).fraction / Weight(2).fraction)
    Weight(2) / Weight(1) shouldBe Weight.MaxValue
    Weight(1) / Weight.MinValue shouldBe Weight.MaxValue
    Weight(1) / Weight.MaxValue shouldBe Weight(1)
  }

  it should "implement equals correctly" in {
    Weight(1) shouldBe Weight(1)
    Weight(1) shouldNot be(Weight(2))
  }

  it should "implement compare correctly" in {
    Weight(1) should be <= Weight(2)
  }
}
