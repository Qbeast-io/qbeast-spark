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

import io.qbeast.core.transform.LinearTransformer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RevisionTest extends AnyFlatSpec with Matchers {

  "Revision" should "check indexing columns correctly" in {
    val revision = Revision(
      1L,
      1L,
      QTableId(""),
      1,
      Vector(LinearTransformer("col_1", IntegerDataType)),
      Vector.empty)

    revision.matchColumns(Seq("col_1")) shouldBe true
    revision.matchColumns(Seq("col_1:linear")) shouldBe true

    revision.matchColumns(Seq("col_2")) shouldBe false
    revision.matchColumns(Seq("col_1", "col_2")) shouldBe false
    revision.matchColumns(Seq("col_1:histogram")) shouldBe false
  }

}
