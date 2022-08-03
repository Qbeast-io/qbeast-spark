package io.qbeast.spark.internal.sources.catalog

import io.qbeast.context.QbeastContext
import io.qbeast.spark.internal.QbeastOptions.checkQbeastProperties
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisExceptionFactory, V1TableQbeast}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

class QbeastCatalog extends DelegatingCatalogExtension {

  private val tableFactory = QbeastContext.indexedTableFactory

  /**
   * Checks if the Table is created with Qbeast Format
   * @param properties the map of properties of the table
   * @return a boolean set to true in the case it's a Qbeast formatted table
   */
  protected def isQbeastTableProvider(properties: Map[String, String]): Boolean = {
    properties.get("provider") match {
      case Some("qbeast") => true
      case _ => false
    }
  }

  /**
   * Creates a qbeast table based on the underlying table
   * @param table the underlying table
   * @return a Table with Qbeast information and implementations
   */
  protected def qbeastTable(table: Table): Table = {

    val prop = table.properties().asScala.toMap
    val schema = table.schema()

    if (isQbeastTableProvider(prop)) {
      table match {
        case V1TableQbeast(v1Table) =>
          checkQbeastProperties(prop)
          val catalogTable = v1Table.v1Table

          val path: String = if (catalogTable.tableType == CatalogTableType.EXTERNAL) {
            // If it's an EXTERNAL TABLE, we can find the path through the Storage Properties
            catalogTable.storage.locationUri.get.toString
          } else if (catalogTable.tableType == CatalogTableType.MANAGED) {
            // If it's a MANAGED TABLE, the location is set in the former catalogTable
            catalogTable.location.toString
          } else {
            // Otherwise, TODO
            throw AnalysisExceptionFactory.create("No path found for table " + table.name())
          }
          new QbeastTableImpl(new Path(path), prop, Some(schema), tableFactory)

        case _ => table
      }
    } else table
  }

  override def loadTable(ident: Identifier): Table = {
    val superTable = super.loadTable(ident)
    qbeastTable(superTable)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val superTable = super.createTable(ident, schema, partitions, properties)
    qbeastTable(superTable)
  }

}
