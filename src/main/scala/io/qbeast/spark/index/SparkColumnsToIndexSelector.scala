/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.ColumnsToIndexSelector
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.Pipeline
import org.apache.spark.qbeast.config.MAX_NUM_COLUMNS_TO_INDEX
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.DataFrame

object SparkColumnsToIndexSelector extends ColumnsToIndexSelector[DataFrame] with Serializable {

  /**
   * The maximum number of columns to index.
   *
   * @return
   */
  override def MAX_COLUMNS_TO_INDEX: Int = MAX_NUM_COLUMNS_TO_INDEX

  /**
   * Adds unix timestamp columns to the DataFrame for the columns specified
   * @param data
   * @param inputCols
   * @return
   */
  private def withUnixTimestamp(data: DataFrame, inputCols: Seq[StructField]): DataFrame = {
    val timestampColsTransformation = inputCols
      .filter(_.dataType == TimestampType)
      .map(c => (c.name, unix_timestamp(col(c.name))))
      .toMap

    data.withColumns(timestampColsTransformation)
  }

  /**
   * Adds preprocessing transformers to the DataFrame for the columns specified
   * @param data
   *   the DataFrame
   * @param inputCols
   *   the columns to preprocess
   * @return
   */
  protected def withPreprocessedPipeline(
      data: DataFrame,
      inputCols: Seq[StructField]): DataFrame = {

    val transformers = inputCols
      .collect {
        case column if column.dataType == StringType =>
          val colName = column.name
          val indexer = new StringIndexer().setInputCol(colName).setOutputCol(s"${colName}_Index")
          val encoder =
            new OneHotEncoder().setInputCol(s"${colName}_Index").setOutputCol(s"${colName}_Vec")
          Seq(indexer, encoder)

        case column =>
          val colName = column.name
          Seq(
            new VectorAssembler()
              .setInputCols(Array(colName))
              .setOutputCol(s"${colName}_Vec")
              .setHandleInvalid("keep"))
      }
      .flatten
      .toArray

    val preprocessingPipeline = new Pipeline().setStages(transformers)
    val preprocessingModel = preprocessingPipeline.fit(data)
    val preprocessedData = preprocessingModel.transform(data)

    preprocessedData
  }

  /**
   * Calculates the top N minimum absolute correlated columns
   * @param data
   *   the DataFrame
   * @param inputCols
   *   the columns to preprocess
   * @param numCols
   *   the number of columns to return
   * @return
   */
  protected def getTopNCorrelatedColumns(
      data: DataFrame,
      inputCols: Seq[StructField],
      numCols: Int): Array[String] = {

    val inputVecCols = inputCols.map(_.name + "_Vec").toArray

    val assembler = new VectorAssembler()
      .setInputCols(inputVecCols)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val vectorDf = assembler.transform(data)

    // Calculate the correlation matrix
    val correlationMatrix: DataFrame = Correlation.corr(vectorDf, "features")
    // Extract the correlation matrix as a Matrix
    val corrArray = correlationMatrix.select("pearson(features)").head.getAs[Matrix](0)

    // Calculate the average absolute correlation for each column
    val averageCorrelation =
      corrArray.toArray.map(Math.abs).grouped(inputVecCols.length).toArray.head

    // Get the indices of columns with the lowest average correlation
    val sortedIndices = averageCorrelation.zipWithIndex.sortBy { case (corr, _) => corr }
    val selectedIndices = sortedIndices.take(numCols).map(_._2)

    val selectedCols = selectedIndices.map(inputCols(_).name)
    selectedCols

  }

  override def selectColumnsToIndex(data: DataFrame, numColumnsToIndex: Int): Seq[String] = {

    // IF there's no data to write, we return all the columns to index
    if (data.isEmpty) {
      return data.columns.take(numColumnsToIndex)
    }

    val inputCols = data.schema
    // Add unix timestamp columns
    val updatedData = withUnixTimestamp(data, inputCols)
    // Add column transformers
    val preprocessedPipeline = withPreprocessedPipeline(updatedData, inputCols)
    // Calculate the top N minimum absolute correlated columns
    val selectedColumns =
      getTopNCorrelatedColumns(preprocessedPipeline, inputCols, numColumnsToIndex)

    selectedColumns

  }

}
