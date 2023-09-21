package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.ByteArrayInputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream

class IndexFileTest extends AnyFlatSpec with Matchers {

  "IndexFile" should "support Java serialization" in {
    val file = File("path", 1, 2)
    val cubeId = CubeId.root(1)
    val state = "FLOODED"
    val blocks = Array(
      Block(file, RowRange(3, 4), cubeId, state, Weight(5), Weight(6)),
      Block(file, RowRange(7, 8), cubeId, state, Weight(9), Weight(10)),
      Block(file, RowRange(11, 12), cubeId, state, Weight(13), Weight(14)),
      Block(file, RowRange(15, 16), cubeId, state, Weight(17), Weight(18)),
      Block(file, RowRange(19, 20), cubeId, state, Weight(21), Weight(22)))

    val indexFile = IndexFile(file, 23, blocks)

    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(indexFile)
    out.close()

    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))
    val indexFile2 = in.readObject()
    in.close()

    indexFile shouldBe indexFile2
  }

  it should "know its revision" in {
    val file = File("path", 1, 2)
    val cubeId = CubeId.root(1)
    val state = "FLOODED"
    val blocks = Array(Block(file, RowRange(3, 4), cubeId, state, Weight(5), Weight(6)))
    val indexFile = IndexFile(file, 23, blocks)
    indexFile.belongsToRevision(23) shouldBe true
    indexFile.belongsToRevision(24) shouldBe false
  }

  it should "know cubes of its blocks" in {
    val file = File("path", 1, 2)
    val rootCubeId = CubeId.root(1)
    val childCubeId = rootCubeId.firstChild
    val state = "FLOODED"
    val blocks = Array(
      Block(file, RowRange(3, 4), rootCubeId, state, Weight(5), Weight(6)),
      Block(file, RowRange(7, 8), rootCubeId, state, Weight(9), Weight(10)),
      Block(file, RowRange(11, 12), childCubeId, state, Weight(13), Weight(14)),
      Block(file, RowRange(15, 16), childCubeId, state, Weight(17), Weight(18)),
      Block(file, RowRange(19, 20), childCubeId, state, Weight(21), Weight(22)))

    val indexFile = IndexFile(file, 23, blocks)

    indexFile.hasBlocksFromCubes(Set(rootCubeId)) shouldBe true
    indexFile.hasBlocksFromCubes(Set(childCubeId)) shouldBe true
    indexFile.hasBlocksFromCubes(Set(childCubeId.firstChild)) shouldBe false
  }

  it should "set replicated blocks correctly" in {
    val file = File("path", 1, 2)
    val rootCubeId = CubeId.root(1)
    val childCubeId = rootCubeId.firstChild
    val state = "FLOODED"
    val blocks = Array(
      Block(file, RowRange(3, 4), rootCubeId, state, Weight(5), Weight(6)),
      Block(file, RowRange(7, 8), rootCubeId, state, Weight(9), Weight(10)),
      Block(file, RowRange(11, 12), childCubeId, state, Weight(13), Weight(14)),
      Block(file, RowRange(15, 16), childCubeId, state, Weight(17), Weight(18)),
      Block(file, RowRange(19, 20), childCubeId, state, Weight(21), Weight(22)))

    var indexFile = IndexFile(file, 23, blocks)

    indexFile = indexFile.setBlocksReplicated(Set(rootCubeId))

    indexFile.blocks.foreach { block =>
      if (block.cubeId == rootCubeId) {
        block.state shouldBe "REPLICATED"
      } else {
        block.state shouldBe "FLOODED"
      }
    }
  }

  it should "compute element count correctly" in {
    val file = File("path", 1, 2)
    val rootCubeId = CubeId.root(1)
    val childCubeId = rootCubeId.firstChild
    val state = "FLOODED"
    val blocks = Array(
      Block(file, RowRange(3, 4), rootCubeId, state, Weight(5), Weight(6)),
      Block(file, RowRange(7, 8), rootCubeId, state, Weight(9), Weight(10)),
      Block(file, RowRange(11, 12), childCubeId, state, Weight(13), Weight(14)),
      Block(file, RowRange(15, 16), childCubeId, state, Weight(17), Weight(18)),
      Block(file, RowRange(19, 20), childCubeId, state, Weight(21), Weight(22)))

    val indexFile = IndexFile(file, 23, blocks)

    indexFile.elementCount shouldBe blocks.map(_.elementCount).sum
  }
}
