package io.qbeast.spark.sql.connector.catalog

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.SparkDataSourceUtils._
import org.apache.spark.sql.sources.{DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class QbeastPhotonDatasource extends TableProvider with DataSourceRegister {

  private lazy val sparkSession = SparkSession.active

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    val scalaOptions = options.asScala.toMap
    val path = getPath(scalaOptions)
    val name = getTableName(scalaOptions, Seq(path), shortName())
    QbeastTable(name, sparkSession, scalaOptions, path, None).schema()
  }

  override def getTable(
      structType: StructType,
      transforms: Array[Transform],
      options: util.Map[String, String]): Table = {

    val scalaOptions = options.asScala.toMap
    val path = getPath(scalaOptions)
    val name = getTableName(scalaOptions, Seq(path), shortName())
    QbeastTable(name, sparkSession, scalaOptions, path, Some(structType))
  }

  override def shortName(): String = "qbeast"
}
