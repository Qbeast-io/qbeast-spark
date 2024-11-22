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

import io.qbeast.core.model._
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import io.qbeast.IISeq
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Analyzes the data and extracts OTree structures
 */
trait OTreeDataAnalyzer {

  /**
   * This method calculates the required indexes updates required after adding to the index the
   * new data.
   *
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current status of the index
   * @return
   *   the changes to the index
   */
  def analyzeAppend(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges)

  /**
   * This method calculates that new cube id association of the provided data once it is
   * optimized. It also calculates the new domains.
   *
   * @param dataToOptimize
   *   the data we want to optimize
   * @param indexStatus
   *   the index changes in the domain after optimizing
   * @return
   *   the dataframe with the new Cube and weight column. This dataframe is cached, and should be
   *   un-persisted once is used.
   */
  def analyzeOptimize(
      dataToOptimize: DataFrame,
      indexStatus: IndexStatus): (DataFrame, TableChanges)

}

object DoublePassOTreeDataAnalyzer extends OTreeDataAnalyzer with Serializable with Logging {

  /**
   * Estimates MaxWeight on DataFrame
   */

  /**
   * Analyze a specific group of columns of the dataframe and extract valuable statistics
   * @param data
   *   the data to analyze
   * @param columnTransformers
   *   the columns to analyze
   * @return
   */
  private[index] def getDataFrameStats(
      data: DataFrame,
      columnTransformers: IISeq[Transformer]): Row = {
    val columnStats = columnTransformers.map(_.stats)
    val columnsExpr = columnStats.flatMap(_.statsSqlPredicates)
    logInfo("Computing statistics: " + columnsExpr.mkString(", "))
    val row = data.selectExpr(columnsExpr ++ Seq("count(1) AS count"): _*).first()
    logInfo("Computed statistics: " + row.mkString(", "))
    row
  }

  /**
   * Given a Row with Statistics, outputs the RevisionChange
   * @param row
   *   the row with statistics
   * @param revision
   *   the current revision
   * @return
   */
  private[index] def calculateRevisionChanges(
      row: Row,
      revision: Revision): Option[RevisionChange] = {
    // TODO: When all indexing columns are provided with a boundary, a new revision is
    //  created directly. If the actual data boundaries are not contained by those
    //  values, the RevisionID would then increase again by 1, leaving a discontinued
    //  sequence of RevisionIDs in the metadata.

    val newTransformation =
      revision.columnTransformers.map(_.makeTransformation(colName => row.getAs[Object](colName)))

    val transformationDelta = if (revision.transformations.isEmpty) {
      newTransformation.map(a => Some(a))
    } else {
      revision.transformations.zip(newTransformation).map {
        case (oldTransformation, newTransformation)
            if oldTransformation.isSupersededBy(newTransformation) =>
          Some(oldTransformation.merge(newTransformation))
        case _ => None
      }
    }

    if (transformationDelta.flatten.isEmpty) {
      None
    } else {
      Some(
        RevisionChange(
          supersededRevision = revision,
          timestamp = System.currentTimeMillis(),
          transformationsChanges = transformationDelta))

    }

  }

  private[index] def addRandomWeight(revision: Revision): DataFrame => DataFrame =
    (df: DataFrame) => {
      df.withColumn(
        weightColumnName,
        qbeastHash(revision.columnTransformers.map(name => df(name.columnName)): _*))
    }

  private[index] def computePartitionCubeDomains(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isNewRevision: Boolean): DataFrame => Dataset[CubeDomain] =
    (weightedDataFrame: DataFrame) => {
      val spark = SparkSession.active
      import spark.implicits._

      val cols = revision.columnTransformers.map(_.columnName) :+ weightColumnName
      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val bufferCapacity: Long = CUBE_WEIGHTS_BUFFER_CAPACITY

      // Broadcast large objects for CubeDomainsBuilder.
      // The index should be built from scratch if it is a new revision
      val startingCubeWeights =
        if (isNewRevision) Map.empty[CubeId, Weight]
        else indexStatus.cubeMaxWeights()
      val broadcastExistingCubeWeights = spark.sparkContext.broadcast(startingCubeWeights)

      val selected = weightedDataFrame.select(cols.map(col): _*)
      val weightIndex = selected.schema.fieldIndex(weightColumnName)
      selected
        .mapPartitions(rows => {
          val domains = CubeDomainsBuilder(
            existingCubeWeights = broadcastExistingCubeWeights.value,
            desiredCubeSize = revision.desiredCubeSize,
            numPartitions = numPartitions,
            numElements = numElements,
            bufferCapacity = bufferCapacity)
          rows.foreach { row =>
            val point = RowUtils.rowValuesToPoint(row, revision)
            val weight = Weight(row.getAs[Int](weightIndex))
            domains.update(point, weight)
          }
          domains.result().iterator
        })

    }

  /**
   * Compute cube domains for the input data. It starts with building an OTree for each input data
   * partition, the result of which is a Dataset of (CubeId, domain) pairs from all partitions.
   * The cube domains from all partitions are then merged using a group by cube and sum. *
   */
  private[index] def computeInputDataCubeDomains(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isNewRevision: Boolean): DataFrame => Dataset[(CubeId, Double)] =
    weightedDataFrame => {
      // Compute cube domains by building an OTree for each input data partition
      // The result is a Dataset of (CubeId, domain) pairs from all partitions
      val inputPartitionCubeDomains =
        weightedDataFrame.transform(
          computePartitionCubeDomains(
            numElements: Long,
            revision: Revision,
            indexStatus: IndexStatus,
            isNewRevision))

      // Merge the cube domains from all partitions
      import weightedDataFrame.sparkSession.implicits._
      inputPartitionCubeDomains
        .groupBy("cubeBytes")
        .agg(sum("domain"))
        .as[(Array[Byte], Double)]
        .map { row => (revision.createCubeId(row._1), row._2) }
    }

  /**
   * Update the OTree index cube domains by merging the existing cube domains with that from the
   * input data.
   */
  private[index] def computeUpdatedCubeDomains(
      inputDataCubeDomains: Map[CubeId, Double],
      indexStatus: IndexStatus,
      isNewRevision: Boolean): Map[CubeId, Double] = {
    if (isNewRevision || indexStatus.cubesStatuses.isEmpty) inputDataCubeDomains
    else {
      val existingCubeDomains = indexStatus.cubeDomains()
      (existingCubeDomains.keys ++ inputDataCubeDomains.keys).map { cubeId: CubeId =>
        val existingDomain = existingCubeDomains.getOrElse(cubeId, 0d)
        val addedDomain = inputDataCubeDomains.getOrElse(cubeId, 0d)
        cubeId -> (existingDomain + addedDomain)
      }.toMap
    }
  }

  /**
   * Avoid computing the weight for the current cube if any of its ancestors is leaf.
   * @param cube
   *   the current CubeId whose NormalizedWeight is of our interest
   * @param cubeNormalizedWeights
   *   existing NormalizedWeights
   * @return
   */
  private[index] def skipCube(
      cube: CubeId,
      cubeNormalizedWeights: Map[CubeId, NormalizedWeight]): Boolean = {
    if (cube.isRoot) false // should always compute weight for the root
    else if (!cubeNormalizedWeights.contains(cube.parent.get)) true
    else cubeNormalizedWeights(cube.parent.get) >= 1d
  }

  /**
   * Populate updated NormalizedWeights in a top-down fashion using the updated cube domains: Wc =
   * Wpc + desiredCubeSize / domain. When treeSize <= desiredCubeSize, we force a leaf.
   * @param updatedCubeDomains
   *   updated cube domain
   * @param indexStatus
   *   existing index metadata
   * @param revisionToUse
   *   the revision to use
   * @param isNewRevision
   *   whether the current append is triggering the creation of a new revision
   * @return
   */
  private[index] def estimateUpdatedCubeWeights(
      updatedCubeDomains: Seq[(CubeId, Double)],
      indexStatus: IndexStatus,
      revisionToUse: Revision,
      isNewRevision: Boolean): Map[CubeId, Weight] = {
    var cubeNormalizedWeights = Map.empty[CubeId, NormalizedWeight]
    val desiredCubeSize = revisionToUse.desiredCubeSize
    val levelCubes = updatedCubeDomains.groupBy(_._1.depth)
    val (minLevel, maxLevel) = (levelCubes.keys.min, levelCubes.keys.max)
    (minLevel to maxLevel).foreach { level =>
      levelCubes(level)
        .filterNot(cd => skipCube(cd._1, cubeNormalizedWeights))
        .foreach { case (cube, domain) =>
          val parentWeight = cube.parent match {
            case Some(parent) => cubeNormalizedWeights(parent)
            case None => 0d
          }
          val treeSize = domain * (1d - parentWeight).max(1d)
          val normalizedWeight =
            if (treeSize <= desiredCubeSize) 1d // force leaf
            else parentWeight + NormalizedWeight(desiredCubeSize, domain.toLong)
          cubeNormalizedWeights += (cube -> normalizedWeight)
        }
    }
    cubeNormalizedWeights.map { case (cubeId, nw) =>
      (cubeId, NormalizedWeight.toWeight(nw))
    }
  }

  override def analyzeAppend(
      dataFrame: DataFrame,
      indexStatus: IndexStatus): (DataFrame, TableChanges) = {
    logTrace(s"Begin: Analyzing the input data with existing revision: ${indexStatus.revision}")
    // Compute the statistics for the indexedColumns
    val dataFrameStats = getDataFrameStats(dataFrame, indexStatus.revision.columnTransformers)
    val numElements = dataFrameStats.getAs[Long]("count")
    val spaceChanges = calculateRevisionChanges(dataFrameStats, indexStatus.revision)
    val (isNewRevision, revisionToUse) = spaceChanges match {
      case None => (false, indexStatus.revision)
      case Some(revisionChange) => (true, revisionChange.createNewRevision)
    }
    logDebug(s"revisionToUse=$revisionToUse")

    // Add a random weight column
    val weightedDataFrame = dataFrame.transform(addRandomWeight(revisionToUse))

    // Compute input data cube domains
    logDebug(s"Computing cube domains for the input data")
    val inputDataCubeDomains: Map[CubeId, Double] =
      weightedDataFrame
        .transform(
          computeInputDataCubeDomains(numElements, revisionToUse, indexStatus, isNewRevision))
        .collect()
        .toMap

    // Merge input cube domains with the existing cube domains
    logDebug(s"Computing the updated cube domains")
    val updatedCubeDomains: Map[CubeId, Double] =
      computeUpdatedCubeDomains(inputDataCubeDomains, indexStatus, isNewRevision)

    // Populate NormalizedWeight level-wise from top to bottom
    logDebug(s"Estimating the updated cube weights")
    val updatedCubeWeights: Map[CubeId, Weight] =
      estimateUpdatedCubeWeights(
        updatedCubeDomains.toSeq,
        indexStatus,
        revisionToUse,
        isNewRevision)

    // Compute the number of elements in each block for the current append
    logDebug(s"Estimating the number of elements in each block")
    val inputDataBlockElementCounts =
      computeInputDataBlockElementCounts(inputDataCubeDomains, updatedCubeWeights)

    // Gather the new changes
    val tableChanges = BroadcastTableChanges(
      spaceChanges,
      indexStatus,
      updatedCubeWeights,
      inputDataBlockElementCounts)
    logTrace(s"End: Analyzing the input data with existing revision: ${indexStatus.revision}")
    (weightedDataFrame, tableChanges)
  }

  private[index] def computeInputDataBlockElementCounts(
      inputDataCubeDomains: Map[CubeId, Double],
      updatedCubeWeights: Map[CubeId, Weight]): Map[CubeId, Long] = {
    inputDataCubeDomains.map { case (cubeId, domain) =>
      val minWeight = getMinWeight(updatedCubeWeights, cubeId).fraction
      val maxWeight = getMaxWeight(updatedCubeWeights, cubeId).fraction
      cubeId -> ((maxWeight - minWeight) * domain).toLong
    }
  }

  private def getMinWeight(cubeWeights: Map[CubeId, Weight], cubeId: CubeId): Weight = {
    cubeId.parent match {
      case Some(parentCubeId) => getMaxWeight(cubeWeights, parentCubeId)
      case None => Weight.MinValue
    }
  }

  private def getMaxWeight(cubeWeights: Map[CubeId, Weight], cubeId: CubeId): Weight = {
    cubeWeights.getOrElse(cubeId, Weight.MaxValue)
  }

  def analyzeOptimize(
      dataToOptimize: DataFrame,
      indexStatus: IndexStatus): (DataFrame, TableChanges) = {
    val spark = dataToOptimize.sparkSession
    val revision = indexStatus.revision
    logTrace(s"""Begin: Analyze Optimize for index with
                |revision=$revision""".stripMargin.replaceAll("\n", " "))

    // Add a random weight column
    val weightedDataFrame = dataToOptimize.transform(addRandomWeight(revision))

    val cubeMaxWeightsBroadcast: Broadcast[Map[CubeId, Weight]] =
      spark.sparkContext.broadcast(indexStatus.cubeMaxWeights())

    val indexedColumns = revision.columnTransformers.map(_.columnName)
    // TODO there function should be rewritten to be faster.
    val dataFrameWithCube = weightedDataFrame
      .withColumn(
        QbeastColumns.cubeColumnName,
        getCubeIdUDF(revision, cubeMaxWeightsBroadcast)(
          struct(indexedColumns.map(col): _*),
          col(QbeastColumns.weightColumnName)))
      .cache()

    import spark.implicits._
    val optimizedDataBlockSizes: Map[CubeId, Long] = dataFrameWithCube
      .groupBy(cubeColumnName)
      .count()
      .as[(Array[Byte], Long)]
      .map(row => revision.createCubeId(row._1) -> row._2)
      .collect()
      .toMap
    val optimizedBlockElementCountsBroadcast =
      spark.sparkContext.broadcast(optimizedDataBlockSizes)
    val tableChanges = BroadcastTableChanges(
      isNewRevision = false,
      revision,
      cubeMaxWeightsBroadcast,
      optimizedBlockElementCountsBroadcast)

    (dataFrameWithCube, tableChanges)
  }

  private def getCubeIdUDF(
      revision: Revision,
      cubeMaxWeightsBroadcast: Broadcast[Map[CubeId, Weight]]): UserDefinedFunction =
    udf { (row: Row, weight: Int) =>
      // in this way, we are serializing in the UDF the broadcast reference, not the Map.
      val cubeMaxWeights = cubeMaxWeightsBroadcast.value
      val point = RowUtils.rowValuesToPoint(row, revision)
      val cubeId = CubeId.containers(point).find { cubeId =>
        cubeMaxWeights.get(cubeId) match {
          case Some(maxWeight) => weight <= maxWeight.value
          case None => true
        }
      }
      cubeId.get.bytes
    }

}
