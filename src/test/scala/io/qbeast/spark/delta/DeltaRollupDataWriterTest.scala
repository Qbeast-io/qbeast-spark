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

import io.qbeast.core.model.BroadcastTableChanges
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.transform.EmptyTransformer
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._

import scala.reflect.io.Path

class DeltaRollupDataWriterTest extends QbeastIntegrationTestSpec {

  "RollupDataWriter" should "write the data correctly" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      import spark.implicits._
      val df = spark
        .range(size)
        .map(i =>
          Client4(
            i * i,
            s"student-$i",
            Some(i.toInt),
            Some(i * 1000 + 123),
            Some(i * 2567.3432143)))
        .toDF()

      val tableID = QTableID(tmpDir)
      val parameters: Map[String, String] =
        Map("columnsToIndex" -> "age,val2", "cubeSize" -> cubeSize.toString)
      val revision =
        SparkRevisionFactory.createNewRevision(tableID, df.schema, QbeastOptions(parameters))
      val indexStatus = IndexStatus(revision)
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)

      val fileActions = DeltaRollupDataWriter.write(tableID, df.schema, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
      }
    }

  it should "compute rollup correctly when optimizing" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val revision =
        Revision(1L, 0, QTableID(tmpDir), 20, Vector(EmptyTransformer("col_1")), Vector.empty)

      val root = revision.createCubeIdRoot()
      val c1 = root.children.next()
      val c2 = c1.nextSibling.get

      val tc = BroadcastTableChanges(
        None,
        IndexStatus.empty(revision),
        Map.empty,
        Map(root -> 20L, c1 -> 1L, c2 -> 20L),
        isOptimizationOperation = false)

      val rollup = DeltaRollupDataWriter.computeRollup(tc)
      rollup shouldBe Map(root -> root, c1 -> root, c2 -> c2)
    }

}
