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
package io.qbeast.core.transform

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.transform.HistogramTransformer.defaultStringHistogram
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmptyTransformationTest extends AnyFlatSpec with Matchers {

  it should "always map to the same value" in {
    val t = EmptyTransformation()

    (1 to 100).foreach { i =>
      t.transform(i) shouldBe 0d
    }

    t.transform(null) shouldBe 0d
  }

  it should "be superseded by another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val lt = LinearTransformation(1d, 1.1, DoubleDataType)
    val sht = StringHistogramTransformation(defaultStringHistogram)

    et.isSupersededBy(ht) shouldBe true
    et.isSupersededBy(lt) shouldBe true
    et.isSupersededBy(sht) shouldBe true
  }

  it should "return the other Transformation when merging" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val lt = LinearTransformation(1d, 1.1, DoubleDataType)
    val sht = StringHistogramTransformation(defaultStringHistogram)

    et.merge(ht) shouldBe ht
    et.merge(lt) shouldBe lt
    et.merge(sht) shouldBe sht
  }

}
