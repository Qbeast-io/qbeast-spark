package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Random

class QbeastSparkIntegrationTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createStudentsTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "The QbeastDataSource" should
    "work with DataFrame API" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createStudentsTestData(spark)
        data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        indexed.count() shouldBe data.count()

        indexed.columns.toSet shouldBe data.columns.toSet

        assertSmallDatasetEquality(
          indexed,
          data,
          orderedComparison = false,
          ignoreNullable = true)

      }
    }

  it should "work with SaveAsTable" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val data = createStudentsTestData(spark)
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .saveAsTable("qbeast")

      val indexed = spark.read.table("qbeast")

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

      assertSmallDatasetEquality(indexed, data, orderedComparison = false, ignoreNullable = true)

    }
  }

  it should "work with Location" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val data = createStudentsTestData(spark)
      val location = tmpDir + "/external"
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("location", location)
        .saveAsTable("qbeast")

      val indexed = spark.read.format("qbeast").load(location)

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

      assertSmallDatasetEquality(indexed, data, orderedComparison = false, ignoreNullable = true)

    }
  }

  it should "work with InsertInto" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val data = createStudentsTestData(spark)
      val location = tmpDir + "/external"
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("location", location)
        .saveAsTable("qbeast")

      val newData = data
      newData.write.insertInto("qbeast")

      val indexed = spark.read.table("qbeast")
      val allData = data.union(data)

      indexed.count() shouldBe allData.count()

      indexed.columns.toSet shouldBe allData.columns.toSet

      assertSmallDatasetEquality(
        indexed,
        allData,
        orderedComparison = false,
        ignoreNullable = true)
    }
  }

  it should "work with path and saveAsTable" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      val data = createStudentsTestData(spark)
      data.createOrReplaceTempView("data")
      spark
        .sql("SELECT * FROM data")
        .write
        .option("columnsToIndex", "id,name")
        .option("path", tmpDir)
        .mode("overwrite")
        .format("qbeast")
        .saveAsTable("data_qbeast")

      val indexed = spark.read.format("qbeast").load(tmpDir)

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

      assertSmallDatasetEquality(indexed, data, orderedComparison = false, ignoreNullable = true)
    })

  // TODO
  it should "work without providing columnsToIndex" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.autoIndexerEnabled", "true")) {
    (spark, tmpDir) =>
      {
        val data = createStudentsTestData(spark)
        data.write.format("qbeast").save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        indexed.count() shouldBe data.count()

        indexed.columns.toSet shouldBe data.columns.toSet

        assertSmallDatasetEquality(
          indexed,
          data,
          orderedComparison = false,
          ignoreNullable = true)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.indexedColumns() shouldBe Seq("id")
        qbeastTable.latestRevisionID() shouldBe 1L

      }
  }

}
