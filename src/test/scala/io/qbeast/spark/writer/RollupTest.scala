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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests of Rollup.
 */
class RollupTest extends AnyFlatSpec with Matchers {

  "Rollup" should "work correctly with basic cube structure" in {
    //                     root(100)
    //                         |
    //                      c0(100)
    //             /       |       \       \
    //         c00(50)   c01(50)  c02(50)  c03(50)
    val root = CubeId.root(2)
    val c0 = root.firstChild
    val Seq(c00, c01, c02, c03) = c0.children.toSeq

    val rollup = new Rollup(100)
    rollup.populate(root, 100)
    rollup.populate(c0, 100)
    rollup.populate(c00, 50)
    rollup.populate(c01, 50)
    rollup.populate(c02, 50)
    rollup.populate(c03, 50)

    rollup.compute() shouldBe Map(
      root -> root,
      c0 -> c0,
      c00 -> c01,
      c01 -> c01,
      c02 -> c03,
      c03 -> c03)
  }

  it should "rollup a cube up to the parent after checking all sibling cubes" in {
    //                     root(100)
    //             /       |       \       \
    //          c0(20)   c1(20)   c2(20)   c3(20)
    val root = CubeId.root(2)
    val Seq(c0, c1, c2, c3) = root.children.toSeq

    val rollup = new Rollup(100)
    rollup.populate(root, 100)
    rollup.populate(c0, 20)
    rollup.populate(c1, 20)
    rollup.populate(c2, 20)
    rollup.populate(c3, 20)

    rollup.compute() shouldBe Map(root -> root, c0 -> root, c1 -> root, c2 -> root, c3 -> root)
  }

  it should "handle empty rollup" in {
    val result = new Rollup(3).compute()
    result shouldBe empty
  }

  it should "handle single cube" in {
    val root = CubeId.root(1)
    val result = new Rollup(3)
      .populate(root, 2)
      .compute()

    result(root) shouldBe root
  }

}
