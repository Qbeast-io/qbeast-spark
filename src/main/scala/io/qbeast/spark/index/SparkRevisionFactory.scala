/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{QDataType, QTableID, Revision, RevisionFactory, RevisionID}
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.SparkToQTypesUtils
import io.qbeast.core.transform.{EmptyTransformation, Transformation, Transformer}
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionFactory extends RevisionFactory[StructType] {

  val SpecExtractor: Regex = "([^:]+):([A-z]+)".r

  def getColumnQType(columnName: String, schema: StructType): QDataType = {
    SparkToQTypesUtils.convertDataTypes(schema(columnName).dataType)
  }

  override def createNewRevision(
      qtableID: QTableID,
      schema: StructType,
      options: Map[String, String]): Revision = {

    val qbeastOptions = QbeastOptions(options)
    val columnSpecs = qbeastOptions.columnsToIndex
    val desiredCubeSize = qbeastOptions.cubeSize
    val stats = qbeastOptions.stats

    val transformers = columnSpecs.map {
      case SpecExtractor(columnName, transformerType) =>
        Transformer(transformerType, columnName, getColumnQType(columnName, schema))

      case columnName =>
        Transformer(columnName, getColumnQType(columnName, schema))

    }.toVector

    val transformations =
      if (stats.isEmpty) Vector.empty
      else {

        val builder = Vector.newBuilder[Transformation]
        builder.sizeHint(transformers.size)

        transformers.foreach(transformer => {
          val rowStats = stats.first()
          // We are going to try if the column is present in the stats option
          if (rowStats.schema.exists(_.name.contains(transformer.columnName))) {
            builder += transformer.makeTransformation(columnName =>
              rowStats.getAs[Object](columnName))
          } else {
            // If it's not present on the dataframe,
            // we can return an empty transformation that will be directly superseed
            builder += EmptyTransformation()
          }
        })
        builder.result()
      }

    Revision.firstRevision(qtableID, desiredCubeSize, transformers, transformations)

  }

  override def createNextRevision(
      qtableID: QTableID,
      schema: StructType,
      options: Map[String, String],
      oldRevisionID: RevisionID): Revision = {
    val revision = createNewRevision(qtableID, schema, options)
    revision.copy(revisionID = oldRevisionID + 1)
  }

}
