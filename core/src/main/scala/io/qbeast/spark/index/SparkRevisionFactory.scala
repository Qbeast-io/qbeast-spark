/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionFactory
import io.qbeast.core.model.RevisionID
import io.qbeast.core.transform.EmptyTransformation
import io.qbeast.core.transform.ManualColumnStats
import io.qbeast.core.transform.ManualPlaceholderTransformation
import io.qbeast.core.transform.Transformation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionFactory extends RevisionFactory {

  /**
   * Creates a new revision
   *
   * The Revision is created with the provided QbeastOptions: cubeSize, columnsToIndex and
   * columnStats
   *
   *   - For each column to index, a Transformer is created.
   *   - For each Transformer, we look for manual column stats.
   *   - If no column stats are provided, and manual stats are required, we use a
   *     ManualPlaceholderTransformation.
   *   - If manual column stats are provided, we create a Transformation with boundaries.
   *   - If no column stats are provided, and no manual stats are required, we use an
   *     EmptyTransformation.
   * @param qtableID
   *   the table identifier
   * @param schema
   *   the schema
   * @param options
   *   the options
   * @return
   */
  override def createNewRevision(
      qtableID: QTableID,
      schema: StructType,
      options: QbeastOptions): Revision = {

    val desiredCubeSize = options.cubeSize
    val columnsToIndex = options.columnsToIndexParsed
    val transformers = columnsToIndex.map(_.toTransformer(schema)).toVector

    // Check if the columns to index are present in the schema
    var shouldCreateNewSpace = true
    val manualDefinedColumnStats = options.columnStats.isDefined
    val columnStats = if (manualDefinedColumnStats) {
      val spark = SparkSession.active
      import spark.implicits._
      spark.read
        .option("inferTimestamp", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
        .json(Seq(options.columnStats.get).toDS())
        .first()
    } else Row.empty

    val transformations = {
      val builder = Vector.newBuilder[Transformation]
      builder.sizeHint(transformers.size)

      transformers.foreach(transformer => {
        // A Transformer needs manual column stats if:
        // - it's type is ManualColumnStats
        val needManualColumnStats = transformer.stats match {
          case _: ManualColumnStats => true
          case _ => false
        }
        val hasManualColumnStats = manualDefinedColumnStats &&
          columnStats.schema.exists(_.name.contains(transformer.columnName))
        if (hasManualColumnStats) {
          // If manual column stats are provided
          // Create transformation with boundaries
          builder += transformer.makeTransformation(columnName =>
            columnStats.getAs[Object](columnName))
        } else if (needManualColumnStats) {
          // If no column stats are provided, and manual stats are required
          // Use an ManualPlaceholderTransformation which will throw an error when indexing
          builder += ManualPlaceholderTransformation(
            transformer.columnName,
            transformer.stats.statsNames)
          shouldCreateNewSpace = false
        } else {
          // If no column stats are provided, and no manual stats are required
          // Use an EmptyTransformation which will always be superseded
          builder += EmptyTransformation()
          shouldCreateNewSpace = false
        }
      })
      builder.result()
    }

    val revision =
      Revision.firstRevision(qtableID, desiredCubeSize, transformers, transformations)

    // When all indexing columns have been provided with a boundary, update the RevisionID
    // to 1 to avoid using the StagingRevisionID(0). It is possible for this RevisionID to
    // to be later updated to 2 if the actual column boundaries are larger that than the
    // provided values. In this case, we will have Revision 2 instead of Revision 1.
    if (shouldCreateNewSpace) revision.copy(revisionID = 1)
    else revision
  }

  override def createNextRevision(
      qtableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      oldRevisionID: RevisionID): Revision = {
    val revision = createNewRevision(qtableID, schema, options)
    revision.copy(revisionID = oldRevisionID + 1)
  }

}
