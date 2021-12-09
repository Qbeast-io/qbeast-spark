package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.OTreeIndex
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

class QbeastFilterPushdownTest extends QbeastIntegrationTestSpec {

  private val filter_user_greaterThanOrEq = "(`user_id` >= 536764969)"
  private val filter_user_lessThan = "(`user_id` < 546280860)"
  private val filter_product_lessThan = "(`product_id` >= 11522682)"
  private val filter_product_greaterThanOrEq = "(`product_id` < 50500010)"

  private def checkFileFiltering(query: DataFrame): Unit = {
    val leaves = query.queryExecution.executedPlan.collectLeaves()

    leaves.exists(p =>
      p
        .asInstanceOf[FileSourceScanExec]
        .relation
        .location
        .isInstanceOf[OTreeIndex]) shouldBe true

    leaves
      .foreach {
        case f @ FileSourceScanExec(
              HadoopFsRelation(index: OTreeIndex, _, _, _, _, _),
              _,
              _,
              _,
              _,
              _,
              _,
              _,
              _) =>
          val matchingFiles =
            index.listFiles(f.partitionFilters, f.dataFilters).flatMap(_.files)
          val allFiles = index.inputFiles
          matchingFiles.length shouldBe <(allFiles.length)
      }

  }

  "Qbeast" should
    "return a valid filtering of the original dataset " +
    "for one column" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val filters = Seq(filter_user_lessThan, filter_user_greaterThanOrEq)
        val filter = filters.mkString(" and ")
        val qbeastQuery = df.filter(filter)
        val normalQuery = data.filter(filter)

        checkFileFiltering(qbeastQuery)
        qbeastQuery.count() shouldBe normalQuery.count()
        assertLargeDatasetEquality(qbeastQuery, normalQuery, orderedComparison = false)

      }
    }

  it should
    "return a valid filtering of the original dataset " +
    "for all columns indexed" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val filters = Seq(
          filter_user_lessThan,
          filter_user_greaterThanOrEq,
          filter_product_lessThan,
          filter_product_greaterThanOrEq)
        val filter = filters.mkString(" and ")
        val qbeastQuery = df.filter(filter)
        val normalQuery = data.filter(filter)

        checkFileFiltering(qbeastQuery)
        qbeastQuery.count() shouldBe normalQuery.count()
        assertLargeDatasetEquality(qbeastQuery, normalQuery, orderedComparison = false)

      }
    }

  "Logical Optimization" should
    "pushdown the filters to the datasource" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)

          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val df = spark.read.format("qbeast").load(tmpDir)

          val filters = Seq(
            filter_user_lessThan,
            filter_user_greaterThanOrEq,
            filter_product_lessThan,
            filter_product_greaterThanOrEq)
          val filter = filters.mkString(" and ")
          val query = df.selectExpr("*").sample(0.1).filter(filter)

          val leaves = query.queryExecution.executedPlan.collectLeaves()

          val dataFilters = leaves
            .collectFirst {
              case f: FileSourceScanExec if f.relation.location.isInstanceOf[OTreeIndex] =>
                f.dataFilters.filterNot(_.isInstanceOf[QbeastMurmur3Hash])
            }
            .getOrElse(Seq.empty)

          val dataFiltersSql = dataFilters.map(_.sql)

          filters.foreach(filter => dataFiltersSql should contain(filter))
        }

    }
}
