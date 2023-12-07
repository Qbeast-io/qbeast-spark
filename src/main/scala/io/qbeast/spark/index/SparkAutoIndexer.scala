/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.AutoIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.Pipeline
import org.apache.spark.qbeast.config.MAX_NUM_COLUMNS_TO_INDEX
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.DataFrame

object SparkAutoIndexer extends AutoIndexer[DataFrame] with Serializable {

  /**
   * The maximum number of columns to index.
   *
   * @return
   */
  override def MAX_COLUMNS_TO_INDEX: Int = MAX_NUM_COLUMNS_TO_INDEX

  /**
   * Chooses the columns to index without limiting the number of columns.
   *
   * @param data
   *   the data to index
   * @return
   */
  override def chooseColumnsToIndex(data: DataFrame): Seq[String] =
    chooseColumnsToIndex(data, MAX_COLUMNS_TO_INDEX)

  override def chooseColumnsToIndex(
      data: DataFrame,
      numColumnsToIndex: Int = MAX_NUM_COLUMNS_TO_INDEX): Seq[String] = {

    // IF there's no data to write, we return all the columns to index
    if (data.isEmpty) {
      return data.columns.take(numColumnsToIndex)
    }

    var updatedData = data
    val inputCols = data.columns

    // Convert timestamp columns to Unix timestamps and update the DataFrame
    val timestampCols = inputCols.filter(colName =>
      data.schema(colName).dataType == org.apache.spark.sql.types.TimestampType)
    timestampCols.foreach { colName =>
      updatedData = updatedData.withColumn(colName, unix_timestamp(col(colName)))
    }

    // Create a list of transformers for string columns
    val stringTransformers = inputCols.collect {
      case colName
          if updatedData.schema(colName).dataType == org.apache.spark.sql.types.StringType =>
        val indexer = new StringIndexer().setInputCol(colName).setOutputCol(s"${colName}_Index")
        val encoder =
          new OneHotEncoder().setInputCol(s"${colName}_Index").setOutputCol(s"${colName}_Vec")
        Seq(indexer, encoder)
    }.flatten

    // Create a list of transformers for non-string columns
    val nonStringTransformers = inputCols.collect {
      case colName
          if updatedData.schema(colName).dataType != org.apache.spark.sql.types.StringType =>
        new VectorAssembler()
          .setInputCols(Array(colName))
          .setOutputCol(s"${colName}_Vec")
          .setHandleInvalid("keep")
    }

    // Combine all transformers
    val transformers = (stringTransformers ++ nonStringTransformers).toArray

    // Create a pipeline for preprocessing
    val preprocessingPipeline = new Pipeline().setStages(transformers)
    val preprocessingModel = preprocessingPipeline.fit(updatedData)
    val preprocessedData = preprocessingModel.transform(updatedData)

    // VectorAssembler to combine features into a single vector column
    val inputVecCols = inputCols.map(colName => s"${colName}_Vec").toArray // Convert to Array
    val assembler = new VectorAssembler()
      .setInputCols(inputVecCols)
      .setOutputCol("features")
      .setHandleInvalid("keep")
    val vectorDf = assembler.transform(preprocessedData)

    // Calculate the correlation matrix
    val correlationMatrix: DataFrame = Correlation.corr(vectorDf, "features")

    // Extract the correlation matrix as a Matrix
    val corrArray = correlationMatrix.select("pearson(features)").head.getAs[Matrix](0)

    // Get the column names and their corresponding correlation values
    val columnNames = inputVecCols

    // Calculate the average absolute correlation for each column
    val averageCorrelation =
      corrArray.toArray.map(Math.abs).grouped(columnNames.length).toArray.head

    // Get the indices of columns with the lowest average correlation
    val sortedIndices = averageCorrelation.zipWithIndex.sortBy { case (corr, _) => corr }
    val selectedIndices = sortedIndices.take(numColumnsToIndex).map(_._2)

    // Create a mapping from transformed column names to original column names
    val transformedToOriginal = inputCols.flatMap { colName =>
      val transformedName = s"${colName}_Vec"
      Some(transformedName -> colName)
    }.toMap

    // Select the columns based on the selected indices
    val selectedTransformedColumns = selectedIndices.map(columnNames(_))

    // Map the selected transformed column names back to original column names
    val selectedOriginalColumns = selectedTransformedColumns.map(transformedToOriginal)

    selectedOriginalColumns
  }

}
