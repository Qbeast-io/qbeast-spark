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

import io.qbeast.core.model._
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.PrivateMethodTester

class CubeDomainsSrcTest extends QbeastIntegrationTestSpec with PrivateMethodTester {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143))
      .toDF()
      .as[Client3]
  }

  "CubeWeights" should
    "reflect the estimation through the Delta Commit Log" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          val df = createDF(100000)
          val names = List("age", "val2")
          val indexStatus = IndexStatus(
            SparkRevisionFactory
              .createNewRevision(
                QTableID("test"),
                df.schema,
                QbeastOptions(
                  Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))))
          val (_, tc) = oTreeAlgorithm.index(df.toDF(), indexStatus)
          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", "age,val2")
            .save(tmpDir)

          val tableId = new QTableID(tmpDir)
          val qbeastSnapshot = QbeastSnapshot("delta", tableId)
          val commitLogWeightMap = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

          // commitLogWeightMap shouldBe weightMap
          commitLogWeightMap.keys.foreach(cubeId => {
            tc.cubeWeight(cubeId) should be('defined)
          })
        }

    }

  it should "respect the (0.0, 1.0] range" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    val df = createDF(100000)
    val names = List("age", "val2")

    df.write
      .format("qbeast")
      .mode("overwrite")
      .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
      .save(tmpDir)

    val tableId = new QTableID(tmpDir)
    val qbeastSnapshot = QbeastSnapshot("delta", tableId)
    val cubeWeights = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

    cubeWeights.values.foreach { case CubeStatus(_, weight, _, _, _) =>
      weight shouldBe >(Weight.MinValue)
      weight shouldBe <=(Weight.MaxValue)
    }
  }

  it should "respect the lower bound for groupCubeSize(1000)" in withSpark { _ =>
    val numElements =
      DEFAULT_CUBE_SIZE * CUBE_WEIGHTS_BUFFER_CAPACITY / CubeDomainsBuilder.minGroupCubeSize
    val numPartitions = 1
    val estimateGroupCubeSize = PrivateMethod[Int]('estimateGroupCubeSize)

    // numElements = 5e11 > 5e8 => groupCubeSize < 1000 => groupCubeSize = 1000
    CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
      DEFAULT_CUBE_SIZE,
      numPartitions,
      numElements * 1000,
      CUBE_WEIGHTS_BUFFER_CAPACITY) shouldBe CubeDomainsBuilder.minGroupCubeSize

    // numElements = 5e8 => groupCubeSize = 1000
    CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
      DEFAULT_CUBE_SIZE,
      numPartitions,
      numElements,
      CUBE_WEIGHTS_BUFFER_CAPACITY) shouldBe CubeDomainsBuilder.minGroupCubeSize

    // numElements = 5e6 < 5e8 => groupCubeSize > 1000
    CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
      DEFAULT_CUBE_SIZE,
      numPartitions,
      numElements / 100,
      CUBE_WEIGHTS_BUFFER_CAPACITY) shouldBe >(CubeDomainsBuilder.minGroupCubeSize)
  }

}
