/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[RowRange]].
 */
class RowRangeTest extends AnyFlatSpec with Matchers {

  "RowRange" should "be empty if 'from' and 'to' are equal" in {
    RowRange(0, 0).isEmpty shouldBe true
    RowRange(1, 1).isEmpty shouldBe true
    RowRange(1, 1).isEmpty shouldBe true
  }

  it should "be non-empty if 'from' is less than 'to'" in {
    RowRange(0, 1).isEmpty shouldBe false
  }

  it should "have correct length as difference between 'from' and 'to'" in {
    RowRange(0, 0).length shouldBe 0
    RowRange(0, 1).length shouldBe 1
  }

  it should "contain value between 'from' and 'to'" in {
    RowRange(0, 0).contains(0) shouldBe false
    RowRange(0, 1).contains(0) shouldBe true
    RowRange(0, 2).contains(1) shouldBe true
    RowRange(1, 2).contains(0) shouldBe false
    RowRange(1, 2).contains(3) shouldBe false
  }
}
