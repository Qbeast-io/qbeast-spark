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
import io.qbeast.core.model.QbeastColumnStats
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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

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
trait SparkRevisionChangesUtils extends StagingUtils with Logging {

  /**
   * Compute the changes between the existing revision, the input data, and the user input
   * configurations
   * @param revision
   *   the existing revision
   * @param options
   *   the QbeastOptions
   * @param data
   *   the input data
   * @return
   *   a tuple with the revision changes and the number of elements in the input data
   */
  def computeRevisionChanges(
      revision: Revision,
      options: QbeastOptions,
      data: DataFrame): (Option[RevisionChange], Long) = {
    checkColumnChanges(revision, options)
    // 1. Compute transformer changes
    val transformerChanges =
      computeTransformerChanges(revision.columnTransformers, options, data.schema)
    // 2. Update transformers if necessary
    val updatedTransformers =
      computeUpdatedTransformers(revision.columnTransformers, transformerChanges)
    // 3. Get the stats from the DataFrame
    val dataFrameStats = getDataFrameStats(data, updatedTransformers)
    val numElements = dataFrameStats.getAs[Long]("count")
    // 4. Compute the cube size changes
    val cubeSizeChanges = computeCubeSizeChanges(revision, options)
    // 5. Compute the Transformation changes given the input data and the user input
    val transformationChanges =
      computeTransformationChanges(
        updatedTransformers,
        revision.transformations,
        options,
        dataFrameStats,
        data.schema)
    // 6. Return RevisionChanges.
    //
    // Revision should change if:
    //    - Cube Size has changed
    //    - Transformer types had changed
    //    - Transformations have changed
    val hasRevisionChanges =
      cubeSizeChanges.isDefined ||
        transformerChanges.flatten.nonEmpty ||
        transformationChanges.flatten.nonEmpty
    val revisionChanges = if (hasRevisionChanges) {
      val rc = RevisionChange(
        supersededRevision = revision,
        timestamp = System.currentTimeMillis(),
        desiredCubeSizeChange = cubeSizeChanges,
        columnTransformersChanges = transformerChanges,
        transformationsChanges = transformationChanges)
      Some(rc)
    } else None
    (revisionChanges, numElements)
  }

  /**
   * Check if the columns to index have changed
   * @param revision
   *   the existing revision
   * @param options
   *   the QbeastOptions
   */
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

  /**
   * Compute the transformer changes for the current operation. The input column names are assumed
   * to be the same as the existing ones. A new Transformer will be created if the existing one is
   * an EmptyTransformer or if the transformerType has changed.To change the transformer type, one
   * has to specify exactly the transformerType in the columnToIndex option:
   * option("columnsToIndex", "col_name:transformer_type"). If the transformerType is not
   * specified, no transformer change will be detected.
   *
   * @param transformers
   *   the existing transformers
   * @param options
   *   the QbeastOptions
   * @param schema
   *   the data schema
   */
  private[index] def computeTransformerChanges(
      transformers: IISeq[Transformer],
      options: QbeastOptions,
      schema: StructType): IISeq[Option[Transformer]] = {
    val newColumnSpecs = options.columnsToIndexParsed
    val existingColumnSpecs = transformers.map(t => ColumnToIndex(t.spec))
    newColumnSpecs
      .zip(existingColumnSpecs)
      .map {
        case (newSpec, ColumnToIndex(_, Some(oldType)))
            if oldType == EmptyTransformer.transformerSimpleName =>
          // If the existing Transformer is an EmptyTransformer
          Some(newSpec.toTransformer(schema))
        case (newSpec @ ColumnToIndex(_, Some(newType)), ColumnToIndex(_, Some(oldType)))
            if newType != oldType => // If the transformerType has changed
          Some(newSpec.toTransformer(schema))
        case _ => None
      }
      .toIndexedSeq
  }

  /**
   * Compute the updated transformers. If a new transformer is created, it will replace the
   * existing one.
   * @param transformers
   *   the existing transformers
   * @param transformerChanges
   *   the transformer changes created by user input
   * @return
   */
  private[index] def computeUpdatedTransformers(
      transformers: IISeq[Transformer],
      transformerChanges: IISeq[Option[Transformer]]): IISeq[Transformer] = {
    transformers.zip(transformerChanges).map {
      case (_, Some(newT)) => newT
      case (t, None) => t
    }
  }

  /**
   * Analyze and extract statistics for the indexing columns
   * @param data
   *   the data to analyze
   * @param transformers
   *   the columns to analyze
   */
  private[index] def getDataFrameStats(data: DataFrame, transformers: IISeq[Transformer]): Row = {
    val columnsStatsExpr = transformers.map(_.stats).flatMap(_.statsSqlPredicates)
    logInfo("Computing statistics: " + columnsStatsExpr.mkString(", "))
    val row = data.selectExpr(columnsStatsExpr ++ Seq("count(1) AS count"): _*).first()
    logInfo("Computed statistics: " + row.mkString(", "))
    row
  }

  /**
   * Compute the cube size changes for the current operation
   * @param revision
   *   the existing revision
   * @param options
   *   the QbeastOptions
   */
  private[index] def computeCubeSizeChanges(
      revision: Revision,
      options: QbeastOptions): Option[Int] = {
    if (revision.desiredCubeSize != options.cubeSize) Some(options.cubeSize)
    else None
  }

  /**
   * Compute the transformation changes for the current operation. The process of creating
   * transformation changes is as follows:
   *   - Compute the Transformations from the input columnStats(if available) and the input
   *     DataFrame statistics.
   *   - Check if the new transformations supersede the existing ones.
   *   - Make sure no EmptyTransformation is used. If a CDFQuantilesTransformers is present, it
   *     must be properly setup.
   * @param transformers
   *   the updated transformers
   * @param transformations
   *   the existing transformations
   * @param options
   *   the QbeastOptions
   * @param row
   *   the Row containing column statistics computed from the input DataFrame
   */
  private[index] def computeTransformationChanges(
      transformers: IISeq[Transformer],
      transformations: IISeq[Transformation],
      options: QbeastOptions,
      row: Row,
      dataSchema: StructType): IISeq[Option[Transformation]] = {
    // Compute transformations from dataFrameStats
    val transformationsFromDataFrameStats =
      computeTransformationsFromDataFrameStats(transformers, row)

    // Compute transformations from columnStats
    val transformationsFromColumnsStats =
      computeTransformationsFromColumnStats(transformers, options, dataSchema)

    // Merge transformations from DataFrame and columnStats
    val newTransformations = transformationsFromDataFrameStats
      .zip(transformationsFromColumnsStats)
      .map {
        case (tdf, Some(tcs)) => tdf.merge(tcs)
        case (tdf, None) => tdf
      }
    // Create transformationChanges
    val transformationChanges =
      if (transformations.isEmpty) newTransformations.map(Some(_))
      else {
        // If the input revision has transformations, check if the new transformations supersede them.
        transformations.zip(newTransformations).map {
          case (oldT, newT) if oldT.isSupersededBy(newT) => Some(oldT.merge(newT))
          case _ => None
        }
      }
    // Make sure no EmptyTransformation is used.
    // If a CDFQuantilesTransformers is present, it must be properly setup
    transformers.zip(transformationChanges).foreach {
      case (t, Some(_: EmptyTransformation)) =>
        var msg = s"Empty transformation for column ${t.columnName}."
        if (t.isInstanceOf[CDFQuantilesTransformer]) {
          msg += s" The following must be provided to use QuantileTransformers: ${t.stats.statsNames.head}."
        }
        throw AnalysisExceptionFactory.create(msg)
      case _ =>
    }
    transformationChanges
  }

  /**
   * Compute transformations from the input column stats. If the stats are not available, the
   * transformation is ignored.
   * @param transformers
   *   the transformers
   * @param options
   *   the QbeastOptions containing the columnStats
   */
  private[index] def computeTransformationsFromColumnStats(
      transformers: IISeq[Transformer],
      options: QbeastOptions,
      dataSchema: StructType): IISeq[Option[Transformation]] = {
    // 1. Get the columnStats from the options
    val columnStatsString = options.columnStats.getOrElse("")
    // 2. Build the QbeastColumnStats
    val qbeastColumnStats =
      QbeastColumnStats(columnStatsString, transformers, dataSchema)
    // 3. Compute transformations from the columnStats
    transformers.map(qbeastColumnStats.createTransformation)
  }

  /**
   * Compute transformations from the input DataFrame statistics. If the stats are not available,
   * an EmptyTransformation should be created.
   * @param transformers
   *   the transformers
   * @param row
   *   the Row containing column statistics computed from the input DataFrame
   */
  private[index] def computeTransformationsFromDataFrameStats(
      transformers: IISeq[Transformer],
      row: Row): IISeq[Transformation] = {
    transformers.map(_.makeTransformation(row.getAs[Object]))
  }

}
