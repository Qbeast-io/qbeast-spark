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

import io.qbeast.core.model.ColumnToIndex
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionChange
import io.qbeast.core.model.RevisionFactory
import io.qbeast.core.model.StagingUtils
import io.qbeast.core.transform.CDFQuantilesTransformer
import io.qbeast.core.transform.EmptyTransformation
import io.qbeast.core.transform.EmptyTransformer
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
    checkColumnChanges(revision, options)
    val cubeSizeChange = computeCubeSizeChange(revision, options)
    val transformerChanges = computeTransformerChanges(revision, options, dataSchema)
    val transformationChanges =
      computeTransformationChanges(transformerChanges, revision, options, statsRow)
    val hasChanges =
      cubeSizeChange.isDefined ||
        transformerChanges.flatten.nonEmpty ||
        transformationChanges.flatten.nonEmpty
    if (hasChanges) {
      val rc = RevisionChange(
        supersededRevision = revision,
        timestamp = System.currentTimeMillis(),
        desiredCubeSizeChange = cubeSizeChange,
        columnTransformersChanges = transformerChanges,
        transformationsChanges = transformationChanges)
      Some(rc)
    } else None
  }

  private[index] def checkColumnChanges(revision: Revision, options: QbeastOptions): Unit = {
    val columnChanges = !revision.matchColumns(options.columnsToIndex)
    if (columnChanges) {
      // Currently, we don't allow changing the indexing columns
      val currentColumnsToIndex = revision.columnTransformers.map(_.columnName)
      throw AnalysisExceptionFactory.create(
        s"columnsToIndex: '${options.columnsToIndex.mkString(",")}' does not have " +
          s"the same column names as the existing ones: ${currentColumnsToIndex.mkString(",")}.")
    }
  }

  private[index] def computeCubeSizeChange(
      revision: Revision,
      options: QbeastOptions): Option[Int] = {
    if (revision.desiredCubeSize != options.cubeSize) Some(options.cubeSize)
    else None
  }

  /**
   * Compute the transformer changes for the current operation. The input column names are assumed
   * to be the same as the existing ones. A new Transformer will be created if the existing one is
   * an EmptyTransformer or if the transformerType has changed.
   * @param revision
   *   the existing revision
   * @param options
   *   the QbeastOptions
   * @param schema
   *   the data schema
   */
  private[index] def computeTransformerChanges(
      revision: Revision,
      options: QbeastOptions,
      schema: StructType): IISeq[Option[Transformer]] = {
    val newColumnSpecs = options.columnsToIndexParsed
    val existingColumnSpecs = revision.columnTransformers.map(t => ColumnToIndex(t.spec))
    newColumnSpecs
      .zip(existingColumnSpecs)
      .map {
        case (newSpec, ColumnToIndex(_, Some(EmptyTransformer.transformerSimpleName))) =>
          // If the existing Transformer is an EmptyTransformer
          Some(newSpec.toTransformer(schema))
        case (newSpec @ ColumnToIndex(_, Some(newType)), ColumnToIndex(_, Some(oldType)))
            if newType != oldType => // If the transformerType has changed
          Some(newSpec.toTransformer(schema))
        case _ => None
      }
      .toIndexedSeq
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
   * Compute the transformation changes for the current operation. The process of creating
   * transformation changes is as follows:
   *   1. Compute the transformations from the input columnStats 2. Compute the new
   *      Transformations from the input DataFrame statistics and the columnStats, if available,
   *      and merge them. 4. Check if the new transformations supersede the existing ones
   * @param transformerChanges
   *   the transformer changes
   * @param revision
   *   the existing revision
   * @param options
   *   the QbeastOptions
   * @param row
   *   the Row containing column statistics computed from the input DataFrame
   */
  private[index] def computeTransformationChanges(
      transformerChanges: IISeq[Option[Transformer]],
      revision: Revision,
      options: QbeastOptions,
      row: Row): IISeq[Option[Transformation]] = {
    val transformers = updateTransformers(revision.columnTransformers, transformerChanges)
    val newTransformations = computeNewTransformations(transformers, options, row)
    // Create transformationChanges
    val transformationChanges =
      if (revision.transformations.isEmpty) newTransformations.map(Some(_))
      else {
        // If the input revision has transformations, check if the new transformations supersede them.
        revision.transformations.zip(newTransformations).map {
          case (oldT, newT) if oldT.isSupersededBy(newT) => Some(oldT.merge(newT))
          case _ => None
        }
      }
    // Make sure that CDFQuantilesTransformers, if present, are properly setup
    transformers.zip(transformationChanges).foreach {
      case (t: CDFQuantilesTransformer, Some(_: EmptyTransformation)) =>
        var msg = s"Empty transformation for column: ${t.columnName}."
        if (t.isInstanceOf[CDFQuantilesTransformer]) {
          msg += s"Columns stats required for quantile transformer/transformations for column: ${t.columnName}." +
            s"Please provide the following statistics via columnStats: ${t.stats.statsNames}"
        }
        throw AnalysisExceptionFactory.create(msg)
      case _ =>
    }
    transformationChanges
  }

  /**
   * Compute the new transformations from the input DataFrame statistics and the columnStats, if
   * available, and merge them.
   * @param transformers
   *   the transformers
   * @param options
   *   the QbeastOptions containing the columnStats
   * @param row
   *   the Row containing column statistics computed from the input DataFrame
   */
  private[index] def computeNewTransformations(
      transformers: IISeq[Transformer],
      options: QbeastOptions,
      row: Row): IISeq[Transformation] = {
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
    transformationsFromColumnsStats
      .zip(transformationsFromDataFrame)
      .map {
        case (Some(tcs), tdf) => tcs.merge(tdf)
        case (None, tdf) => tdf
      }
  }

}
