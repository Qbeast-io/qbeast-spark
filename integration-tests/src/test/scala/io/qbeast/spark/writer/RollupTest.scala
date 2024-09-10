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
package io.qbeast.spark.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.writer.Rollup
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests of Rollup.
 */
class RollupTest extends AnyFlatSpec with Matchers {

  "Rollup" should "work correctly" in {
    val root = CubeId.root(1)
    val c0 = root.firstChild
    val c1 = c0.nextSibling.get
    val c00 = c0.firstChild
    val c01 = c00.nextSibling.get
    val c10 = c1.firstChild
    val c11 = c10.nextSibling.get

    val result = new Rollup(3)
      .populate(root, 1)
      .populate(c00, 1)
      .populate(c01, 2)
      .populate(c10, 2)
      .populate(c11, 3)
      .compute()

    result(root) shouldBe root
    result(c00) shouldBe c0
    result(c01) shouldBe c0
    result(c10) shouldBe root
    result(c11) shouldBe c11
  }

}
