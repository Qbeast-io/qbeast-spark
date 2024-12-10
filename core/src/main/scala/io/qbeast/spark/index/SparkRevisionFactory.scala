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
import io.qbeast.core.model.RevisionChange
import io.qbeast.core.model.RevisionFactory
import io.qbeast.core.model.StagingUtils
import io.qbeast.core.transform.CDFQuantilesTransformer
import io.qbeast.core.transform.EmptyTransformation
import io.qbeast.core.transform.Transformation
import io.qbeast.core.transform.Transformer
import io.qbeast.IISeq
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionFactory extends RevisionFactory {

  /**
   * Creates a new revision
   *
   * The Revision is created with the provided QbeastOptions: cubeSize, columnsToIndex
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
    val transformers = options.columnsToIndexParsed.map(_.toTransformer(schema)).toVector
    Revision.firstRevision(qtableID, desiredCubeSize, transformers)
  }

}

/**
 * Computes the changes, if any, between the existing space and that of the input Dataframe and
 * the user input columnStats.
 */
object SparkRevisionChangeFactory extends StagingUtils {

  def create(
      revision: Revision,
      options: QbeastOptions,
      statsRow: Row,
      dataSchema: StructType): Option[RevisionChange] = {
    val isNewColumns = !revision.matchColumns(options.columnsToIndex)
    if (isNewColumns) {
      // Currently, we don't allow changing the columns to index
      val currentColumnsToIndex = revision.columnTransformers.map(_.columnName)
      throw AnalysisExceptionFactory.create(
        s"Columns to index: '${options.columnsToIndex.mkString(",")}' do not match " +
          s"existing indexing columns: ${currentColumnsToIndex.mkString(",")}.")
    }
    val cubeSizeChange = computeCubeSizeChange(revision, options)
    val transformerChanges = computeTransformerChanges(revision, options, dataSchema)
    val updatedTransformers = updateTransformers(revision.columnTransformers, transformerChanges)
    val transformationChanges = computeTransformationChanges(
      updatedTransformers,
      revision.transformations,
      options,
      statsRow)
    val hasChanges =
      cubeSizeChange.isDefined ||
        transformerChanges.flatten.nonEmpty ||
        transformationChanges.flatten.nonEmpty
    if (hasChanges) {
      val rc = RevisionChange(
        supersededRevision = revision,
        timestamp = System.currentTimeMillis(),
        desiredCubeSizeChange = cubeSizeChange,
        transformationsChanges = transformationChanges)
      Some(rc)
    } else None
  }

  private[index] def computeCubeSizeChange(
      revision: Revision,
      options: QbeastOptions): Option[Int] = {
    if (revision.desiredCubeSize != options.cubeSize) Some(options.cubeSize)
    else None
  }

  private[index] def computeTransformerChanges(
      revision: Revision,
      options: QbeastOptions,
      schema: StructType): IISeq[Option[Transformer]] = {
    if (isStaging(revision)) {
      options.columnsToIndexParsed.map(_.toTransformer(schema)).map(Some(_)).toVector
    } else options.columnsToIndexParsed.map(_ => None).toVector
  }

  private[index] def updateTransformers(
      transformers: IISeq[Transformer],
      transformerChanges: IISeq[Option[Transformer]]): IISeq[Transformer] = {
    transformers.zip(transformerChanges).map {
      case (_, Some(newT)) => newT
      case (t, None) => t
    }
  }

  /**
   * Compute the transformation changes for the current operation. At the moment, we don't support
   * changing the indexing columns. The process of creating transformation changes is as follows:
   *   1. Compute the transformations from the input columnStats 2. Compute the transformations
   *      from the input DataFrame 3. Merge the transformations from the columnStats and DataFrame
   *      4. Check if the new transformations supersede the existing ones
   * @param transformers
   * @param transformations
   * @param options
   * @param row
   * @return
   */
  private[index] def computeTransformationChanges(
      transformers: IISeq[Transformer],
      transformations: IISeq[Transformation],
      options: QbeastOptions,
      row: Row): IISeq[Option[Transformation]] = {
    val transformationsFromColumnsStats = {
      val (columnStats, availableColumnStats) =
        if (options.columnStats.isDefined) {
          val spark = SparkSession.active
          import spark.implicits._
          val stats = spark.read
            .option("inferTimestamp", "true")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
            .json(Seq(options.columnStats.get).toDS())
            .first()
          (stats, stats.schema.fieldNames.toSet)
        } else (Row.empty, Set.empty[String])
      transformers.map { t =>
        if (t.stats.statsNames.forall(availableColumnStats.contains)) {
          // Create transformation with columnStats
          Some(t.makeTransformation(columnStats.getAs[Object]))
        } else {
          // Ignore the transformation if the stats are not available
          None
        }
      }
    }
    // Create transformations from the input DataFrame
    val transformationsFromDataFrame = transformers.map(_.makeTransformation(row.getAs[Object]))
    // Merge transformations created from the columnStats and DataFrame
    val newTransformations = transformationsFromColumnsStats
      .zip(transformationsFromDataFrame)
      .map {
        case (Some(tcs), tdf) => Some(tcs.merge(tdf))
        case (None, tdf) => Some(tdf)
      }
    // Create transformationChanges
    val transformationChanges =
      if (transformations.isEmpty) newTransformations
      else {
        // If the input revision has transformations, check if the new transformations supersede them.
        transformations.zip(newTransformations).map {
          case (oldT, Some(newT)) if oldT.isSupersededBy(newT) => Some(oldT.merge(newT))
          case _ => None
        }
      }
    // Make sure that CDFQuantilesTransformers, if present, are properly setup
    transformers.zip(transformationChanges).foreach {
      case (t: CDFQuantilesTransformer, Some(_: EmptyTransformation)) =>
        throw AnalysisExceptionFactory.create(
          s"Columns stats required for quantile transformer/transformations for column: ${t.columnName}." +
            s"Please provide the following statistics: ${t.stats.statsNames}")
      case (t, Some(_: EmptyTransformation)) =>
        throw AnalysisExceptionFactory.create(
          s"Empty transformation for column: ${t.columnName}.")
      case _ =>
    }
    transformationChanges
  }

}
