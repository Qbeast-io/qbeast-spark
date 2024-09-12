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

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.QTableID
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import io.qbeast.TestClasses.Client3
import io.qbeast.spark.snapshot.QbeastSnapshot
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.PrivateMethodTester

class AnalyzeAndOptimizeTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {

  def appendNewRevision(spark: SparkSession, tmpDir: String, multiplier: Int): Int = {
    import spark.implicits._
    val df =
      spark
        .range(100000)
        .flatMap(i =>
          Seq(
            Client3(i, s"student-$i", i.intValue(), (i + 123) * multiplier, i * 4),
            Client3(
              i * i,
              s"student-$i",
              i.intValue(),
              (i * 1000 + 123) * multiplier,
              i * 2567.3432143)))

    val names = List("age", "val2")
    df.write
      .format("qbeast")
      .mode("append")
      .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
      .save(tmpDir)
    names.length
  }

  "Analyze command" should "announce root when the tree is not replicated" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val dimensionCount = appendNewRevision(spark, tmpDir, 1)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      val announcedCubes = qbeastTable.analyze()
      announcedCubes shouldBe Seq(CubeId.root(dimensionCount).string)
  }

  it should "not analyze replicated cubes" in withSparkAndTmpDir { (spark, tmpDir) =>
    appendNewRevision(spark, tmpDir, 1)
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.analyze()
    qbeastTable.optimize()
    qbeastTable.analyze()
    qbeastTable.optimize()

    val tableId = new QTableID(tmpDir)
    val qbeastSnapshot = QbeastSnapshot("delta", tableId)
    val replicatedCubes = qbeastSnapshot.loadLatestIndexStatus.replicatedSet

    val announcedCubes = qbeastTable.analyze()
    announcedCubes.foreach(a => replicatedCubes shouldNot contain(a))
  }

  "Optimize command" should "replicate cubes in announce set" ignore withSparkAndTmpDir {
    (spark, tmpDir) =>
      appendNewRevision(spark, tmpDir, 1)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      (0 to 5).foreach(_ => {
        val announcedCubes = qbeastTable.analyze()
        qbeastTable.optimize()
        val tableId = new QTableID(tmpDir)
        val qbeastSnapshot = QbeastSnapshot("delta", tableId)
        val replicatedCubes =
          qbeastSnapshot.loadLatestIndexStatus.replicatedSet.map(_.string)

        announcedCubes.foreach(r => replicatedCubes should contain(r))
      })
  }

}
