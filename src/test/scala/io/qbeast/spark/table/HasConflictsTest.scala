package io.qbeast.spark.table

import io.qbeast.core.model.{CubeId, QTableID}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class HasConflictsTest extends QbeastIntegrationTestSpec {

  /**
   *  A conflict happens if there
   * are new cubes that have been optimized but they were not announced.
   */

  "Has conflict method" should
    "be true on replicated conflicts" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {

        val data = loadTestData(spark)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.analyze()
        qbeastTable.optimize()

        // If the announced cubes is empty,
        // means that the process is not aware of the cubes optimized in the middle
        // and the conflict is not solvable
        SparkDeltaMetadataManager.hasConflicts(
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

        // If we are aware of the announced cubes,
        // the process has to end successfully and no conflicts are raised
        SparkDeltaMetadataManager.hasConflicts(
          QTableID(tmpDir),
          1L,
          knowAnnounced.map(CubeId(2, _)).toSet, // know announced is set
          Set.empty // old replicated set is empty
        ) shouldBe false

      }
    }
}
