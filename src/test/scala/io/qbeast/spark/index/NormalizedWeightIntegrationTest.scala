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
package io.qbeast.spark.index

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

class NormalizedWeightIntegrationTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143))
      .toDF()
      .as[Client3]
  }

  "CubeNormalizedWeights" should
    "write a the right Weight with a single full file" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        val cubeSize = 10000
        val df = createDF(cubeSize).repartition(1)
        val names = List("age", "val2")
        // val dimensionCount = names.length
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        spark.read.format("qbeast").load(tmpDir).count() shouldBe cubeSize

        val tableId = new QTableID(tmpDir)
        val snapshot = QbeastContext.metadataManager.loadSnapshot(tableId)
        val files = snapshot.loadLatestIndexFiles

        files.count() shouldBe 1
        files.collect
          .flatMap(_.blocks)
          .foreach { block =>
            block.maxWeight.value shouldBe <=(Int.MaxValue)
            block.replicated shouldBe false
          }
    }

  it should
    "normalize weights when cubes are half full" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        val cubeSize = 10000
        val df = createDF(cubeSize / 2).repartition(1)
        val names = List("age", "val2")
        // val dimensionCount = names.length
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)
        val cubeNormalizedWeights =
          qbeastSnapshot.loadLatestIndexStatus.cubeNormalizedWeights

        cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe 2.0)
    }

}
