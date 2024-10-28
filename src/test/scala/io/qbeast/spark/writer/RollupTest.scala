package io.qbeast.spark.writer

import io.qbeast.core.model.CubeId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RollupTest extends AnyFlatSpec with Matchers {

  "Rollup" should "work correctly with basic cube structure" in {
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
    result(c10) shouldBe c11 // rolliing up into the next siblings.
    result(c11) shouldBe c11
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

  it should "roll up to parent when size exceeds limit" in {
    val root = CubeId.root(1)
    val kids = root.children.toSeq
    val child = kids(0)
    val grandChild = kids(1)

    val result = new Rollup(2)
      .populate(root,1)
      .populate(child,2)
      .populate(grandChild, 3) // Exceeds limit
      .compute()

    result(grandChild) shouldBe grandChild
  }


}