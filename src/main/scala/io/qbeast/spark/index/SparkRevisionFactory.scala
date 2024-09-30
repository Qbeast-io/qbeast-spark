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

import io.qbeast.core.model.QTableId
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionFactory
import io.qbeast.core.model.RevisionId
import io.qbeast.core.transform.EmptyTransformation
import io.qbeast.core.transform.Transformation
import io.qbeast.spark.internal.QbeastOptions
import org.apache.spark.sql.types.StructType

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionFactory extends RevisionFactory[StructType, QbeastOptions] {

  override def createNewRevision(
      tableId: QTableId,
      schema: StructType,
      options: QbeastOptions): Revision = {

    val desiredCubeSize = options.cubeSize
    val columnsToIndex = options.columnsToIndexParsed
    val transformers = columnsToIndex.map(_.toTransformer(schema)).toVector

    options.stats match {
      case None => Revision.firstRevision(tableId, desiredCubeSize, transformers)
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
          Revision.firstRevision(tableId, desiredCubeSize, transformers, transformations)

        // When all indexing columns have been provided with a boundary, update the RevisionId
        // to 1 to avoid using the StagingRevisionId(0). It is possible for this RevisionId
        // to be later updated to 2 if the actual column boundaries are larger that than the
        // provided values. In this case, we will have Revision 2 instead of Revision 1.
        if (shouldCreateNewSpace) firstRevision.copy(revisionId = 1)
        else firstRevision
    }
  }

  override def createNextRevision(
      tableId: QTableId,
      schema: StructType,
      options: QbeastOptions,
      oldRevisionId: RevisionId): Revision = {
    val revision = createNewRevision(tableId, schema, options)
    revision.copy(revisionId = oldRevisionId + 1)
  }

}
