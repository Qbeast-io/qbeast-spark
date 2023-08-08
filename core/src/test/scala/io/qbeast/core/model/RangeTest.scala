/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[Range]].
 */
class RangeTest extends AnyFlatSpec with Matchers {

  "Range" should "be empty if 'from' and 'to' are equal" in {
    Range(0, 0).isEmpty shouldBe true
    Range(1, 1).isEmpty shouldBe true
    Range(1, 1).isEmpty shouldBe true
  }

  it should "be non-empty if 'from' is less than 'to'" in {
    Range(0, 1).isEmpty shouldBe false
  }

  it should "have correct length as difference between 'from' and 'to'" in {
    Range(0, 0).length shouldBe 0
    Range(0, 1).length shouldBe 1
  }
}
