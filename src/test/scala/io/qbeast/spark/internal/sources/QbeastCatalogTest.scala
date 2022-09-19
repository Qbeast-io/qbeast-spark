package io.qbeast.spark.internal.sources

import io.qbeast.TestClasses.Student
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

class QbeastCatalogTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "QbeastCatalog" should
    "coexist with Delta tables" in withTmpDir(tmpDir =>
      withExtendedSpark(sparkConf = new SparkConf()
        .setMaster("local[8]")
        .set("spark.sql.extensions", "io.qbeast.spark.internal.QbeastSparkSessionExtension")
        .set("spark.sql.warehouse.dir", tmpDir)
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set(
          "spark.sql.catalog.qbeast_catalog",
          "io.qbeast.spark.internal.sources.catalog.QbeastCatalog"))(spark => {

        val data = createTestData(spark)

        data.write.format("delta").saveAsTable("delta_table") // delta catalog

        // spark.sql("USE CATALOG qbeast_catalog")
        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_catalog.default.qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table("delta_table")
        val qbeastTable = spark.read.table("qbeast_catalog.default.qbeast_table")

        assertSmallDatasetEquality(
          deltaTable,
          qbeastTable,
          orderedComparison = false,
          ignoreNullable = true)

      }))

}
