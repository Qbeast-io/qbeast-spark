package io.qbeast.spark.table

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.util.Random

class IndexedTableTest extends QbeastIntegrationTestSpec {

  private val schemaStudents = StructType(
    List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)))

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createStudentsTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "IndexedTable" should "append an empty dataframe" in withSparkAndTmpDir { (spark, tmpDir) =>
    {
      val location = tmpDir + "/external_student/"
      val qTableID = QTableID(location)
      val indexedTableFactory = QbeastContext.indexedTableFactory
      val indexedTable = indexedTableFactory.getIndexedTable(qTableID)

      val indexOptions = Map("columnsToIndex" -> "id,name")
      val emptyDFWithSchema = spark
        .createDataFrame(spark.sharedState.sparkContext.emptyRDD[Row], schemaStudents)

      indexedTable.save(emptyDFWithSchema, indexOptions, true)

      val qbeastTable = QbeastTable.forPath(spark, location)
      qbeastTable.allRevisions().size shouldBe 1
      qbeastTable.latestRevisionID shouldBe 0L
      qbeastTable.latestRevision.columnTransformers.map(_.columnName) shouldBe List("id", "name")
    }
  }

  it should "overwrite with an empty dataframe" in withSparkAndTmpDir { (spark, tmpDir) =>
    {
      val location = tmpDir + "/external_student/"
      val qTableID = QTableID(location)
      val indexedTableFactory = QbeastContext.indexedTableFactory
      val indexedTable = indexedTableFactory.getIndexedTable(qTableID)

      val indexOptions = Map("columnsToIndex" -> "id,name")
      val studentsData = createStudentsTestData(spark)
      indexedTable.save(studentsData, indexOptions, true)

      // overwrite with empty df
      val emptyDFWithSchema = spark
        .createDataFrame(spark.sharedState.sparkContext.emptyRDD[Row], schemaStudents)
      indexedTable.save(emptyDFWithSchema, indexOptions, false)

      val qbeastTable = QbeastTable.forPath(spark, location)
      qbeastTable.allRevisions().size shouldBe 1
      qbeastTable.latestRevisionID shouldBe 0L
      qbeastTable.latestRevision.columnTransformers.map(_.columnName) shouldBe List("id", "name")
      val qbeastDF = spark.read.format("qbeast").load(location)
      qbeastDF.count() shouldBe 0
    }
  }

  it should "append dataframe" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val location = tmpDir + "/external_student/"
      val qTableID = QTableID(location)
      val indexedTableFactory = QbeastContext.indexedTableFactory
      val indexedTable = indexedTableFactory.getIndexedTable(qTableID)

      val data = createStudentsTestData(spark)
      val indexingParameters = Map("columnsToIndex" -> "id,name")
      indexedTable.save(data, indexingParameters, true)

      val qbeastTable = QbeastTable.forPath(spark, location)
      qbeastTable.allRevisions().size shouldBe 2 // First empty revision + append
      qbeastTable.latestRevisionID shouldBe 1L
      qbeastTable.latestRevision.columnTransformers.map(_.columnName) shouldBe List("id", "name")
    }
  }

  it should "overwrite with dataframe" in withQbeastContextSparkAndTmpWarehouse {
    (spark, tmpDir) =>
      {

        val location = tmpDir + "/external_student/"
        val qTableID = QTableID(location)
        val indexedTableFactory = QbeastContext.indexedTableFactory
        val indexedTable = indexedTableFactory.getIndexedTable(qTableID)

        val firstData = createStudentsTestData(spark)
        val firstIndexingParameters = Map("columnsToIndex" -> "id")
        indexedTable.save(firstData, firstIndexingParameters, true)

        val qbeastTable = QbeastTable.forPath(spark, location)
        qbeastTable.allRevisions().size shouldBe 2L // First empty revision + append
        qbeastTable.latestRevisionID shouldBe 1L
        qbeastTable.latestRevision.columnTransformers.map(_.columnName) shouldBe List("id")

        val data = createStudentsTestData(spark)
        val indexingParameters = Map("columnsToIndex" -> "id,name")
        indexedTable.save(data, indexingParameters, false)

        qbeastTable.allRevisions().size shouldBe 2L // First empty revision + append
        qbeastTable.latestRevisionID shouldBe 1L
        qbeastTable.latestRevision.columnTransformers.map(_.columnName) shouldBe List(
          "id",
          "name")
      }
  }

}
