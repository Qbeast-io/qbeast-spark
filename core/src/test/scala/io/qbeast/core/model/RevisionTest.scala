package io.qbeast.core.model

import io.qbeast.core.transform.LinearTransformer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RevisionTest extends AnyFlatSpec with Matchers {

  "Revision" should "check indexing columns correctly" in {
    val revision = Revision(
      1L,
      1L,
      QTableID(""),
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
