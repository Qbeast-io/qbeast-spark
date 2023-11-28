package io.qbeast.spark.internal.sources.catalog

import io.qbeast.context.QbeastContext
import io.qbeast.spark.table.IndexedTableFactory
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.connector.catalog.SparkCatalogV2Util
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkCatalogUtils
import org.apache.spark.sql.SparkSession

import scala.collection.immutable
import scala.util.Random
import org.apache.spark.sql.connector.catalog.Column

/**
 * A test suite for Catalog tests. It includes:
 *   - Creation of Student's dataframe
 *   - Creation of QbeastCatalog with a delegated session catalog
 *   - Schema of the Student's dataframe
 */
trait CatalogTestSuite {

  val schema: StructType = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

  val columns: Array[Column] = SparkCatalogV2Util.structTypeToV2Columns(schema)

  val defaultNamespace: Array[String] = Array("default")

  val students: immutable.Seq[Student] = {
    1.to(10).map(i => Student(i, i.toString, Random.nextInt()))
  }

  val fakeLogicalWriteInfo: LogicalWriteInfo = new LogicalWriteInfo {
    override def options(): CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()

    override def queryId(): String = "1"

    override def schema(): StructType = CatalogTestSuite.this.schema
  }

  lazy val indexedTableFactory: IndexedTableFactory = QbeastContext.indexedTableFactory

  def sessionCatalog(spark: SparkSession): TableCatalog = {
    SparkCatalogUtils.getV2SessionCatalog(spark).asInstanceOf[TableCatalog]

  }

  def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  def createQbeastCatalog(
      spark: SparkSession): TableCatalog with SupportsNamespaces with StagingTableCatalog = {
    val qbeastCatalog = new QbeastCatalog

    // set default catalog
    qbeastCatalog.setDelegateCatalog(sessionCatalog(spark))

    qbeastCatalog
  }

}
