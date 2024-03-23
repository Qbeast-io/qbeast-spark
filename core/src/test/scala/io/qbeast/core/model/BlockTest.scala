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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests of Block.
 */
class BlockTest extends AnyFlatSpec with Matchers {

  "Block" should "implement equals correctly" in {
    val cubeId = CubeId.root(1)
    val file1 = new IndexFileBuilder()
      .setPath("path1")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      // block1
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      // block2
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      // block3
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(5L)
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      // block4
      .beginBlock()
      .setCubeId(cubeId.firstChild)
      .setElementCount(4L)
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      .result()

    val block1 = file1.blocks(0)
    val block2 = file1.blocks(1)
    val block3 = file1.blocks(2)
    val block4 = file1.blocks(3)

    val file2 = new IndexFileBuilder()
      .setPath("path2")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      // block5
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      .result()

    val block5 = file2.blocks(0)

    block1.equals(block1) shouldBe true
    block1.equals(block2) shouldBe true
    block1.equals(block3) shouldBe false
    block1.equals(block4) shouldBe false
    block1.equals(block5) shouldBe false
  }

  it should "replicate correctly" in {
    val cubeId = CubeId.root(1)
    val file = new IndexFileBuilder()
      .setPath("path")
      .setSize(1L)
      .setModificationTime(2L)
      .setRevisionId(3L)
      .beginBlock()
      .setCubeId(cubeId)
      .setElementCount(4L)
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      .result()

    val block = file.blocks(0)
    val replicatedBlock = block.replicate()
    replicatedBlock.file shouldBe block.file
    replicatedBlock.cubeId shouldBe block.cubeId
    replicatedBlock.elementCount shouldBe block.elementCount
    replicatedBlock.minWeight shouldBe block.minWeight
    replicatedBlock.maxWeight shouldBe block.maxWeight
    replicatedBlock.replicated shouldBe true

    replicatedBlock.replicate() shouldBe replicatedBlock

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
      .setMinWeight(Weight.MinValue)
      .setMaxWeight(Weight.MaxValue)
      .setReplicated(false)
      .endBlock()
      .result()

    val block = file.blocks(0)
    block.toString() shouldNot be("")
  }

}
