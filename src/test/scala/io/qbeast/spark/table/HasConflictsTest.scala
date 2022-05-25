package io.qbeast.spark.table

import io.qbeast.core.model.{CubeId, QTableID}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class HasConflictsTest extends QbeastIntegrationTestSpec {

  "Has conflict method" should
    "be true on replicated conflicts" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.analyze()
        qbeastTable.optimize()

        SparkDeltaMetadataManager.hasConflicts(
          QTableID(tmpDir),
          1L,
          Set.empty, // know announced is empty
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

        SparkDeltaMetadataManager.hasConflicts(
          QTableID(tmpDir),
          1L,
          knowAnnounced.map(CubeId(2, _)).toSet, // know announced is from analyze
          Set.empty // old replicated set is empty
        ) shouldBe false

      }
    }
}
