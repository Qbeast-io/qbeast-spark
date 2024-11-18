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
