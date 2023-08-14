package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[Block]].
 */
class BlockTest extends AnyFlatSpec with Matchers {

  "Block" should "have correct number of elements" in {
    val block =
      Block(File("path", 1, 2), RowRange(0, 1), CubeId.root(1), "FLOODED", Weight(2), Weight(3))
    block.elementCount shouldBe 1
    block.copy(range = RowRange(0, 0)).elementCount shouldBe 0
  }
}
