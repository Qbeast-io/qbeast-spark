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

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.CubeStatus
import io.qbeast.core.model.Weight
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog

class IndexStatusBuilderTest extends QbeastIntegrationTestSpec {

  "IndexBuilder" should "build cube information from DeltaLog" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val data = spark.range(100000).toDF("id")

      // Append data x times
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("cubeSize", "10000")
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val indexStatus =
        DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

      indexStatus.revision.revisionID shouldBe 1
      indexStatus.cubesStatuses.map(_._2.elementCount).sum shouldBe 100000L

      indexStatus.replicatedSet shouldBe Set.empty
      indexStatus.announcedSet shouldBe Set.empty
    })

  it should "work well on appending the same revision" in withSparkAndTmpDir((spark, tmpDir) => {

    val data = spark.range(100000).toDF("id")

    // Append data x times
    data.write
      .format("qbeast")
      .option("columnsToIndex", "id")
      .option("cubeSize", "10000")
      .save(tmpDir)
    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val firstIndexStatus =
      DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus
    data.write
      .format("qbeast")
      .mode("append")
      .option("columnsToIndex", "id")
      .option("cubeSize", "10000")
      .save(tmpDir)
    val secondIndexStatus = DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

    secondIndexStatus.revision.revisionID shouldBe 1
    secondIndexStatus.announcedSet shouldBe Set.empty
    secondIndexStatus.replicatedSet shouldBe Set.empty
    secondIndexStatus.cubesStatuses.foreach { case (cube: CubeId, cubeStatus: CubeStatus) =>
      if (cubeStatus.maxWeight < Weight.MaxValue) {
        firstIndexStatus.cubesStatuses.get(cube) shouldBe defined
        cubeStatus.maxWeight shouldBe <=(firstIndexStatus.cubesStatuses(cube).maxWeight)
      }
    }
  })

}
