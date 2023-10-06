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
    val desiredFileSize = qbeastOptions.fileSize

    val transformers = columnSpecs.map {
      case SpecExtractor(columnName, transformerType) =>
        Transformer(transformerType, columnName, getColumnQType(columnName, schema))

      case columnName =>
        Transformer(columnName, getColumnQType(columnName, schema))

    }.toVector

    qbeastOptions.stats match {
      case None =>
        Revision.firstRevision(qtableID, desiredCubeSize, desiredFileSize, transformers)
      case Some(stats) =>
        val columnStats = stats.first()
        var shouldCreateNewSpace = true
        val transformations = {
          val builder = Vector.newBuilder[Transformation]
          builder.sizeHint(transformers.size)

          transformers.foreach(transformer => {
            if (columnStats.schema.exists(_.name.contains(transformer.columnName))) {
              // Create transformation with provided boundaries
              builder += transformer.makeTransformation(columnName =>
                columnStats.getAs[Object](columnName))
            } else {
              // Use an EmptyTransformation which will always be superseded
              builder += EmptyTransformation()
              shouldCreateNewSpace = false
            }
          })
          builder.result()
        }

        val firstRevision =
          Revision.firstRevision(
            qtableID,
            desiredCubeSize,
            desiredFileSize,
            transformers,
            transformations)

        // When all indexing columns have been provided with a boundary, update the RevisionID
        // to 1 to avoid using the StagingRevisionID(0). It is possible for this RevisionID to
        // to be later updated to 2 if the actual column boundaries are larger that than the
        // provided values. In this case, we will have Revision 2 instead of Revision 1.
        if (shouldCreateNewSpace) firstRevision.copy(revisionID = 1)
        else firstRevision
    }
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
