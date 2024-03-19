/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.spark.internal.sources.catalog.CreationMode
import io.qbeast.spark.internal.sources.catalog.QbeastCatalogUtils
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.StagedTable
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability.V1_BATCH_WRITE
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

/**
 * Qbeast Implementation of StagedTable An StagedTable allows Atomic CREATE TABLE AS SELECT /
 * REPLACE TABLE AS SELECT
 */
private[sources] class QbeastStagedTableImpl(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    tableCreationMode: CreationMode,
    override val properties: util.Map[String, String],
    private val tableFactory: IndexedTableFactory)
    extends StagedTable
    with SupportsWrite {

  lazy val spark: SparkSession = SparkSession.active
  lazy val catalog: SessionCatalog = spark.sessionState.catalog

  // Variables for the creation of the writeBuilder
  var writeOptions: Map[String, String] = Map.empty
  var dataFrame: Option[DataFrame] = None

  override def commitStagedChanges(): Unit = {

    val props = new util.HashMap[String, String]()

    // Options passed in through the SQL API will show up both with an "option." prefix and
    // without in Spark 3.1, so we need to remove those from the properties
    val optionsThroughProperties = properties.asScala.collect {
      case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
    }.toSet
    val sqlWriteOptions = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (!k.startsWith("option.") && !optionsThroughProperties.contains(k)) {
        // Do not add to properties
        props.put(k, v)
      } else if (optionsThroughProperties.contains(k)) {
        sqlWriteOptions.put(k, v)
      }
    }
    if (writeOptions.isEmpty && !sqlWriteOptions.isEmpty) {
      writeOptions = sqlWriteOptions.asScala.toMap
    }
    // Drop the delta configuration check,
    // we pass all the writeOptions to the properties as well
    writeOptions.foreach { case (k, v) => props.put(k, v) }

    // Creates the corresponding table on the Catalog and executes
    // the writing of the dataFrame (if any)
    QbeastCatalogUtils.createQbeastTable(
      ident,
      schema(),
      partitions,
      props,
      writeOptions,
      dataFrame,
      tableCreationMode,
      tableFactory,
      catalog)

  }

  override def abortStagedChanges(): Unit = {
    // Do nothing since any path is created until commitStagedChanges())
  }

  override def name(): String = ident.name()

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = Set(V1_BATCH_WRITE).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {

      override def build(): Write = new V1Write {

        override def toInsertableRelation: InsertableRelation = {
          new InsertableRelation {
            def insert(data: DataFrame, overwrite: Boolean): Unit = {
              dataFrame = Some(data)

            }
          }
        }

      }

    }

}
