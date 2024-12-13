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
package io.qbeast.utils

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.mapper
import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.Revision
import io.qbeast.core.model.StringDataType
import io.qbeast.core.transform.CDFQuantilesTransformer
import io.qbeast.core.transform.CDFStringQuantilesTransformation
import io.qbeast.core.transform.IdentityToZeroTransformation
import io.qbeast.core.transform.IdentityTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.core.transform.NullToZeroTransformation
import io.qbeast.core.transform.StringHistogramTransformation
import io.qbeast.core.transform.StringHistogramTransformer
import io.qbeast.spark.utils.MetadataConfig.revision
import io.qbeast.table.QbeastTable
import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.skipping.MultiDimClusteringFunctions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame

import scala.annotation.nowarn

/**
 * Utility object for indexing methods outside the box
 */
object QbeastUtils extends Logging {

  /**
   * Compute the quantiles for a given column of type String
   *
   * @param df
   *   the DataFrame
   * @param columnName
   *   the name of the column
   */
  private def computeQuantilesForStringColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int): Array[String] = {

    import df.sparkSession.implicits._
    val binStarts = "__bin_starts"
    val stringPartitionColumn =
      MultiDimClusteringFunctions.range_partition_id(col(columnName), numberOfQuantiles)

    val quantiles = df
      .select(columnName)
      .na
      .drop()
      .groupBy(stringPartitionColumn)
      .agg(min(columnName).alias(binStarts))
      .select(binStarts)
      .orderBy(binStarts)
      .as[String]
      .collect()

    log.info(s"String Quantiles for column $columnName: $quantiles")
    quantiles.map(string => s"'$string'")

  }

  private def computeQuantilesForNumericColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int,
      relativeError: Double): Array[Double] = {
    val probabilities = (0 to numberOfQuantiles).map(_ / numberOfQuantiles.toDouble).toArray
    val reducedDF = df.select(columnName)
    val approxQuantile = reducedDF.stat.approxQuantile(columnName, probabilities, relativeError)
    log.info(s"Numeric Quantiles for column $columnName: ${approxQuantile.mkString(",")}")
    approxQuantile
  }

  /**
   * Compute a sequence of numberOfQuantiles (default 50) within a relativeError (default 0.1) for
   * a given column
   *
   * Since computing the quantiles can be expensive, this method is used outside the indexing
   * process.
   *
   * It outputs the quantiles of the column as an Array[value1, value2, value2, ...] of size equal
   * to the numberOfQuantiles parameter.
   *
   * For example:
   *
   * val qbeastTable = QbeastTable.forPath(spark, "path")
   *
   * val quantiles = qbeastTable.computeQuantilesForColumn(df, "column")
   *
   * df.write.format("qbeast") .option("columnsToIndex","column:quantiles")
   * .option("columnStats",s"""{"column_quantiles":quantiles}""").save()
   *
   * @param df
   *   DataFrame
   * @param columnName
   *   Column name
   * @param numberOfQuantiles
   *   Number of Quantiles, default is 50
   * @param relativeError
   *   Relative Error, default is 0.1
   * @return
   */
  def computeQuantilesForColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int = 50,
      relativeError: Double = 0.1): String = {
    require(numberOfQuantiles > 1, "Number of quantiles must be greater than 1")
    // Check if the column exists
    if (!df.columns.contains(columnName)) {
      throw AnalysisExceptionFactory.create(s"Column $columnName does not exist in the dataframe")
    }
    val dataType = df.schema(columnName).dataType

    log.info(s"Computing quantiles for column $columnName with number of bins $numberOfQuantiles")
    // Compute the quantiles based on the data type
    val quantilesArray: Array[_] = dataType match {
      case StringType =>
        computeQuantilesForStringColumn(df, columnName, numberOfQuantiles)
      case _: NumericType =>
        computeQuantilesForNumericColumn(df, columnName, numberOfQuantiles, relativeError)
      case _ =>
        throw AnalysisExceptionFactory.create(
          s"Column $columnName is of type $dataType. " +
            "Only StringType and NumericType columns are supported.")
    }
    // Return the quantiles as a string
    quantilesArray.mkString("[", ", ", "]")
  }

  @Experimental
  @nowarn("cat=deprecation")
  def updateTransformationTypes(revision: Revision): Revision = {
    val updatedTransformers = revision.columnTransformers.map {
      case s: StringHistogramTransformer =>
        CDFQuantilesTransformer(s.columnName, StringDataType)
      case other => other
    }
    val updatedTransformations = revision.transformations
      .zip(revision.columnTransformers)
      .map {
        case (s: StringHistogramTransformation, _) =>
          CDFStringQuantilesTransformation(s.histogram)
        case (i: IdentityToZeroTransformation, linearTransformer: LinearTransformer) =>
          IdentityTransformation(
            i.identityValue,
            linearTransformer.dataType.asInstanceOf[OrderedDataType])
        case (_ @NullToZeroTransformation, linearTransformer: LinearTransformer) =>
          IdentityTransformation(null, linearTransformer.dataType.asInstanceOf[OrderedDataType])
        case (otherTransformation, _) => otherTransformation
      }
    revision
      .copy(columnTransformers = updatedTransformers, transformations = updatedTransformations)
  }

  @Experimental
  @nowarn("cat=deprecation")
  def shouldUpdateRevision(revision: Revision): Boolean = {
    revision.transformations.exists {
      case _: StringHistogramTransformation | _: IdentityToZeroTransformation |
          NullToZeroTransformation =>
        true
      case _ => false
    }
  }

  @Experimental
  def updateTransformationTypes(qbeastTable: QbeastTable): Unit = {
    // 1. Get te Latest Snapshot of the Table
    val latestSnapshot = qbeastTable.getLatestSnapshot
    // 2. Load all the Revisions Present
    val allRevisions = latestSnapshot.loadAllRevisions
    // 3. Check if there are any revisionsToUpdate
    val revisionsToUpdate = allRevisions.filter(shouldUpdateRevision)
    // If there are no revisions to update, return
    if (revisionsToUpdate.isEmpty) {
      log.info("No Revisions to Update")
      return
    }
    // 4. Update all the Revisions with the new Transformation Types
    val allRevisionsUpdated = revisionsToUpdate.map(updateTransformationTypes)
    log.info(
      s"Updating Revisions ${allRevisionsUpdated.map(_.revisionID).toString()} with new Transformation Types")
    // 4. Commit the new Metadata Update
    QbeastContext.metadataManager.updateMetadataWithTransaction(
      qbeastTable.tableID,
      latestSnapshot.schema) {
      val newMetadata = allRevisionsUpdated
        .map(revisionUpdated =>
          s"$revision.${revisionUpdated.revisionID}" -> mapper.writeValueAsString(
            revisionUpdated))
        .toMap
      newMetadata
    }

  }

  /**
   * Compute the histogram for a given column
   *
   * Since computing the histogram can be expensive, this method is used outside the indexing
   * process.
   *
   * It outputs the histogram of the column as format [bin1, bin2, bin3, ...] Number of bins by
   * default is 50
   *
   * For example:
   *
   * val qbeastTable = QbeastTable.forPath(spark, "path")
   *
   * val histogram =qbeastTable.computeHistogramForColumn(df, "column")
   *
   * df.write.format("qbeast").option("columnsToIndex",
   * "column:histogram").option("columnStats",histogram).save()
   *
   * @param df
   * @param columnName
   */
  @deprecated("Use computeQuantilesForColumn method instead", "0.8.0")
  def computeHistogramForColumn(df: DataFrame, columnName: String, numBins: Int = 50): String = {

    import df.sparkSession.implicits._
    if (!df.columns.contains(columnName)) {
      throw AnalysisExceptionFactory.create(s"Column $columnName does not exist in the dataframe")
    }

    log.info(s"Computing histogram for column $columnName with number of bins $numBins")
    val binStarts = "__bin_starts"
    val stringPartitionColumn =
      MultiDimClusteringFunctions.range_partition_id(col(columnName), numBins)

    val histogram = df
      .select(columnName)
      .distinct()
      .na
      .drop()
      .groupBy(stringPartitionColumn)
      .agg(min(columnName).alias(binStarts))
      .select(binStarts)
      .orderBy(binStarts)
      .as[String]
      .collect()

    log.info(s"Histogram for column $columnName: $histogram")
    histogram
      .map(string => s"'$string'")
      .mkString("[", ",", "]")
  }

}
