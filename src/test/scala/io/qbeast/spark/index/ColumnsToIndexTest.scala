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
package io.qbeast.spark.index

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[ColumnsToIndex]].
 */
class ColumnsToIndexTest extends AnyFlatSpec with Matchers {
  "ColumnsToIndex" should "encode and decode columns correctly" in {
    for (columns <- Seq(Seq("A", "B", "C"), Seq("A"))) {
      ColumnsToIndex.decode(ColumnsToIndex.encode(columns)) shouldBe columns
    }
  }

}
