/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.qbeast.core.model.CubeId

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
