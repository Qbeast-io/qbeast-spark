/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for IndexFile.
 */
class IndexFileTest extends AnyFlatSpec with Matchers {

  "IndexFile" should "return the correct number of elements" in {
    val cubeId = CubeId.root(1)
    val file = new IndexFileBuilder()
      .setPath("path")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .endBlock()
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(5L)
      .endBlock()
      .result()
    file.elementCount shouldBe 9L
  }

  it should "detect if it contains cube data" in {
    val cubeId = CubeId.root(1)
    val file = new IndexFileBuilder()
      .setPath("path")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .endBlock()
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(5L)
      .endBlock()
      .result()

    file.hasCubeData(cubeId) shouldBe true
    file.hasCubeData(cubeId.firstChild) shouldBe false
  }

  it should "replicate the specified blocks correctly" in {
    val cubeId = CubeId.root(1)
    val file = new IndexFileBuilder()
      .setPath("path")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .endBlock()
      .beginBlock()
      .setCubeId(cubeId.firstChild)
      .setElementCount(5L)
      .endBlock()
      .result()

    val replicatedFile = file.tryReplicateBlocks(Set(cubeId)).get
    replicatedFile.blocks.filter(_.cubeId == cubeId).foreach(_.replicated shouldBe true)
    replicatedFile.blocks.filterNot(_.cubeId == cubeId).foreach(_.replicated shouldBe false)

    file.tryReplicateBlocks(Set(cubeId.firstChild.nextSibling.get)).isEmpty shouldBe true
  }

  it should "convert to string with toString" in {
    val cubeId = CubeId.root(1)
    val file = new IndexFileBuilder()
      .setPath("path")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .endBlock()
      .beginBlock()
      .setCubeId(cubeId.firstChild)
      .setElementCount(5L)
      .endBlock()
      .result()
    file.toString shouldNot be("")
  }

}
