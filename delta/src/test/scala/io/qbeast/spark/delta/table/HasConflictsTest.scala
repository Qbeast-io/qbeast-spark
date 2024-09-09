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
package io.qbeast.spark.delta.table

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.QTableID
import io.qbeast.spark.delta.DeltaSparkMetadataManager
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable

class HasConflictsTest extends QbeastIntegrationTestSpec {

  /**
   * A conflict happens if there are new cubes that have been optimized but they were not
   * announced.
   */

  "Has conflict method" should
    "be true on replicated conflicts" ignore withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {

        val data = loadTestData(spark)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.analyze()
        qbeastTable.optimize()

        val metadataManager = new DeltaSparkMetadataManager

        // If the announced cubes is empty,
        // means that the process is not aware of the cubes optimized in the middle
        // and the conflict is not solvable
        metadataManager.hasConflicts(
          QTableID(tmpDir),
          1L,
          Set.empty, // knowAnnounced is empty
          Set.empty // old replicated set is empty
        ) shouldBe true

      }
    }

  it should
    "be false when there is no conflict " +
    "between optimization and write" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        val knowAnnounced = qbeastTable.analyze()
        qbeastTable.optimize()

        val metadataManager = new DeltaSparkMetadataManager

        // If we are aware of the announced cubes,
        // the process has to end successfully and no conflicts are raised
        metadataManager.hasConflicts(
          QTableID(tmpDir),
          1L,
          knowAnnounced.map(CubeId(2, _)).toSet, // know announced is set
          Set.empty // old replicated set is empty
        ) shouldBe false

      }
    }

}
