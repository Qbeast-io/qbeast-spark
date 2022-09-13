package io.qbeast.spark.internal.sources

import io.qbeast.TestClasses.Student
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

class QbeastDeltaCatalogTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "QbeastDeltaCatalogTest" should
    "coexist with Delta tables" in withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val data = createTestData(spark)

      data.write.format("delta").saveAsTable("delta_table") // delta catalog

      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .saveAsTable("qbeast_table") // qbeast catalog

      val tables = spark.sessionState.catalog.listTables("default")
      tables.size shouldBe 2

      val deltaTable = spark.read.table("delta_table")
      val qbeastTable = spark.read.table("qbeast_table")

      assertSmallDatasetEquality(
        deltaTable,
        qbeastTable,
        orderedComparison = false,
        ignoreNullable = true)

    })

}
