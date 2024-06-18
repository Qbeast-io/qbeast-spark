package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QTableIDTest extends AnyFlatSpec with Matchers {

  "QTableID" should "create table ID correctly" in {
    val tableID = QTableID("table")
    tableID.id shouldBe
      "table"
    tableID.toString shouldBe "table"
  }

  it should "compare two ids" in {
    val tableID = QTableID("table")
    tableID.equals(tableID) shouldBe true

    val otherTableID = QTableID("table")
    tableID.equals(otherTableID) shouldBe true

    val differentTableID = QTableID("abc")
    tableID.equals(differentTableID) shouldBe false

  }

}
