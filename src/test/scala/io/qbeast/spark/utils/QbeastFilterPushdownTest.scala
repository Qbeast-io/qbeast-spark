package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.OTreeIndex
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.{avg, col, rand, regexp_replace, when}
import org.scalatest.exceptions.TestFailedException

class QbeastFilterPushdownTest extends QbeastIntegrationTestSpec {

  private val filter_user_greaterThanOrEq = "(user_id >= 536764969)"
  private val filter_user_lessThan = "(user_id < 546280860)"
  private val filter_product_lessThan = "(product_id >= 11522682)"
  private val filter_product_greaterThanOrEq = "(product_id < 50500010)"
  private val filter_user_equal = "(user_id = 536764969)"
  private val filter_product_equal = "(product_id = 11522682)"

  private def checkFileFiltering(query: DataFrame): Unit = {
    val leaves =
      query.queryExecution.executedPlan.collectLeaves().filter(_.isInstanceOf[FileSourceScanExec])

    leaves should not be empty

    leaves.exists(p =>
      p
        .asInstanceOf[FileSourceScanExec]
        .relation
        .location
        .isInstanceOf[OTreeIndex]) shouldBe true

    leaves
      .foreach {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[OTreeIndex] =>
          val index = f.relation.location
          val matchingFiles =
            index.listFiles(f.partitionFilters, f.dataFilters).flatMap(_.files)
          val allFiles = index.inputFiles
          matchingFiles.length shouldBe <(allFiles.length)
      }

  }

  private def checkLogicalFilterPushdown(sqlFilters: Seq[String], query: DataFrame): Unit = {
    val leaves = query.queryExecution.sparkPlan.collectLeaves()

    val dataFilters = leaves
      .collectFirst {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[OTreeIndex] =>
          f.dataFilters.filterNot(_.isInstanceOf[QbeastMurmur3Hash])
      }
      .getOrElse(Seq.empty)

    val dataFiltersSql = dataFilters.map(_.sql)
    sqlFilters.foreach(filter => dataFiltersSql should contain(filter))
  }

  "Qbeast" should
    "return a valid filtering of the original dataset " +
    "for one column" in withSparkAndTmpDir { (spark, tmpDir) =>
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
    "for all columns indexed" in withSparkAndTmpDir { (spark, tmpDir) =>
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

  it should
    "return a valid filtering of the original dataset " +
    "for all string columns" in withSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark).na.drop()

        writeTestData(data, Seq("brand", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val filter_brand = "(`brand` == 'versace')"

        val filters = Seq(filter_user_lessThan, filter_user_greaterThanOrEq, filter_brand)
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
    "for range string columns without using Qbeast" in withSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark).na.drop()

        writeTestData(data, Seq("brand", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val filter_brand = "(`brand` > 'versace')"

        val filters = Seq(filter_user_lessThan, filter_user_greaterThanOrEq, filter_brand)
        val filter = filters.mkString(" and ")
        val qbeastQuery = df.filter(filter)
        val normalQuery = data.filter(filter)

        // The file filtering should not be applied in this particular case
        an[TestFailedException] shouldBe thrownBy(checkFileFiltering(qbeastQuery))
        assertLargeDatasetEquality(qbeastQuery, normalQuery, orderedComparison = false)

      }
    }

  it should
    "return a valid filtering of the original dataset " +
    "for null value columns" in withSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        val dataWithNulls =
          data.withColumn(
            "null_product_id",
            when(rand() > 0.5, null).otherwise(col("product_id")))

        writeTestData(dataWithNulls, Seq("brand", "null_product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val filter = "(`null_product_id` is null)"

        val qbeastQuery = df.filter(filter)
        val normalQuery = dataWithNulls.filter(filter)

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
          checkLogicalFilterPushdown(filters, query)

        }

    }

  it should "pushdown filters in complex queries" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val joinData = data.withColumn("price", col("price") * 3)

        val filters = Seq(
          filter_user_lessThan,
          filter_user_greaterThanOrEq,
          filter_product_lessThan,
          filter_product_greaterThanOrEq)
        val filter = filters.mkString(" and ")
        val query = df.union(joinData).filter(filter).agg(avg("price"))

        checkLogicalFilterPushdown(filters, query)

      }

  }

  it should "pushdown filters with OR operator" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val filter = s"($filter_user_equal OR $filter_product_equal)"
        val query = df.filter(filter)
        val originalQuery = data.filter(filter)

        // OR filters are not split, so we need to match them entirely
        checkLogicalFilterPushdown(Seq(filter), query)
        checkFileFiltering(query)
        assertSmallDatasetEquality(query, originalQuery, orderedComparison = false)

      }
  }

  it should "pushdown filters with OR and AND operator" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val filter =
          s"(($filter_user_lessThan AND $filter_user_greaterThanOrEq) " +
            s"OR ($filter_product_greaterThanOrEq AND $filter_product_lessThan))"
        val query = df.filter(filter)
        val originalQuery = data.filter(filter)

        // OR filters are not split, so we need to match them entirely
        checkLogicalFilterPushdown(Seq(filter), query)
        checkFileFiltering(query)
        assertSmallDatasetEquality(query, originalQuery, orderedComparison = false)

      }
  }

  it should "pushdown filters with IN predicate" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val filter =
          s"(user_id IN (555304906, 514439763))"
        val query = df.filter(filter)
        val originalQuery = data.filter(filter)

        // OR filters are not split, so we need to match them entirely
        checkLogicalFilterPushdown(Seq(filter), query)
        checkFileFiltering(query)
        assertSmallDatasetEquality(query, originalQuery, orderedComparison = false)

      }
  }

  it should "pushdown filters with IN predicate with string" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("brand"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val filter =
          s"(brand IN ('versace', 'bioderma'))"
        val query = df.filter(filter)
        val originalQuery = data.filter(filter)

        // OR filters are not split, so we need to match them entirely
        checkLogicalFilterPushdown(Seq(filter), query)
        checkFileFiltering(query)
        assertSmallDatasetEquality(query, originalQuery, orderedComparison = false)

      }
  }

  it should "pushdown regex expressions on strings" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        val columnName = "brand_mod"
        val modifiedData =
          data
            .withColumn(columnName, regexp_replace(col("brand"), "versace", "prefix_versace"))

        // Index data with the new column
        writeTestData(modifiedData, Seq(columnName), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        val regexExpression = "prefix_(.+)"
        val filter =
          s"(regexp_extract($columnName, '$regexExpression', 1) = 'versace')"
        val query = df.filter(filter)
        val originalQuery = modifiedData.filter(filter)
        val originalQueryWithoutRegex = data.filter("brand = 'versace'")

        // OR filters are not split, so we need to match them entirely
        checkLogicalFilterPushdown(Seq(filter), query)
        checkFileFiltering(query)
        assertSmallDatasetEquality(
          query,
          originalQuery,
          orderedComparison = false,
          ignoreNullable = true)
        assertSmallDatasetEquality(
          query.drop(columnName),
          originalQueryWithoutRegex,
          orderedComparison = false,
          ignoreNullable = true)

      }
  }
}
