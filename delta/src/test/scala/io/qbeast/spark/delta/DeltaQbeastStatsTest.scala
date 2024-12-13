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
package io.qbeast.spark.delta

import io.qbeast.core.model.IndexFile
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeltaQbeastStatsTest extends AnyFlatSpec with Matchers {

  "QbeastStats" should "maintain the AddFile stats value" in {

    val jsonString =
      """{"numRecords":52041,"minValues":{"key1": "value1", "key2": {"innerKey": "innerValue"}},
        |"maxValues":{"key3": "value3", "key4": "value4"},"nullCount":{"key5": 0, "key6": 2}}""".stripMargin

    val addFile = AddFile(
      path = "path",
      partitionValues = Map(),
      size = 2L,
      modificationTime = 0L,
      dataChange = true,
      stats = jsonString,
      tags = Map(
        TagUtils.revision -> "1",
        TagUtils.blocks -> // WRONG BLOCK START
          """[{"cubeId":"","minWeight":2147483647,"maxWeight":2147483647,"elementCount":1}]""".stripMargin))

    val indexFile = DeltaQbeastFileUtils.fromAddFile(2)(addFile)
    indexFile.stats.isDefined shouldBe true
    indexFile.stats.get.toString shouldBe jsonString

  }

  it should "map correctly stats from AddFile" in {

    val jsonString =
      """{"numRecords":52041,"minValues":{"key1": "value1", "key2": {"innerKey": "innerValue"}},
        |"maxValues":{"key3": "value3", "key4": "value4"},"nullCount":{"key5": 0, "key6": 2}}""".stripMargin

    val indexFile = IndexFile(
      path = "path",
      size = 2L,
      dataChange = true,
      modificationTime = 0L,
      revisionId = 1L,
      blocks = Seq().toIndexedSeq,
      stats = Some(jsonString))

    val addFile = DeltaQbeastFileUtils.toAddFile(indexFile)
    addFile.stats shouldBe jsonString

  }

}
