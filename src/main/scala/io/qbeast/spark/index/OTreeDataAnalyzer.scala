/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model.{BroadcastedTableChanges, _}
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable.SortedSet

/**
 * Analyzes the data and extracts OTree structures
 */
trait OTreeDataAnalyzer {

  /**
   * Analyze the data to process
   * @param data the data to index
   * @param indexStatus the current status of the index
   * @param isReplication either we are replicating the elements or not
   * @return the changes to the index
   */
  def analyze(
      data: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges)

}

object DoublePassOTreeDataAnalyzer extends OTreeDataAnalyzer with Serializable {

  /**
   * Estimates MaxWeight on DataFrame
   */

  /**
   * Analyze a specific group of columns of the dataframe
   * and extract valuable statistics
   * @param data the data to analyze
   * @param columnTransformers the columns to analyze
   * @return
   */
  private[index] def getDataFrameStats(
      data: DataFrame,
      columnTransformers: IISeq[Transformer]): Row = {
    val columnStats = columnTransformers.map(_.stats)
    val columnsExpr = columnStats.flatMap(_.statsSqlPredicates)
    data.selectExpr(columnsExpr ++ Seq("count(1) AS count"): _*).first()
  }

  /**
   * Given a Row with Statistics, outputs the RevisionChange
   * @param row the row with statistics
   * @param revision the current revision
   * @return
   */
  private[index] def calculateRevisionChanges(
      row: Row,
      revision: Revision): Option[RevisionChange] = {

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

  // DATAFRAME TRANSFORMATIONS //

  private[index] def addRandomWeight(revision: Revision): DataFrame => DataFrame =
    (df: DataFrame) => {
      df.withColumn(
        weightColumnName,
        qbeastHash(revision.columnTransformers.map(name => df(name.columnName)): _*))
    }

  /**
   * Extract data summaries from the data by building Otree indexes on each partition
   */
  private[index] def computeLocalTrees(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean): DataFrame => Dataset[LocalTree] =
    (weightedDataFrame: DataFrame) => {
      val spark = SparkSession.active
      import spark.implicits._

      val indexColumns = if (isReplication) {
        Seq(weightColumnName, cubeToReplicateColumnName)
      } else {
        Seq(weightColumnName)
      }
      val cols = revision.columnTransformers.map(_.columnName) ++ indexColumns

      // Estimate the desiredSize of the cube at partition level
      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val bufferCapacity: Long = CUBE_WEIGHTS_BUFFER_CAPACITY

      val selected = weightedDataFrame
        .select(cols.map(col): _*)
      val weightIndex = selected.schema.fieldIndex(weightColumnName)

      selected
        .mapPartitions(rows => {
          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              numPartitions = numPartitions,
              numElements = numElements,
              bufferCapacity = bufferCapacity)
          rows.foreach { row =>
            val point = RowUtils.rowValuesToPoint(row, revision)
            val weight = Weight(row.getAs[Int](weightIndex))
            if (isReplication) {
              val parentBytes = row.getAs[Array[Byte]](cubeToReplicateColumnName)
              val parent = Some(revision.createCubeId(parentBytes))
              weights.update(point, weight, parent)
            } else weights.update(point, weight)
          }
          weights.result().iterator
        })

    }

  /**
   * Perform cube-wise merge of the local trees to find global cubes and weights.
   * @param localTrees local trees built on the partition data
   * @param desiredCubeSize the desired size of the global cubes
   * @param missingCubeDomain Strategy to use to find the cube domain in a local tree
   * @return
   */
  private[index] def mergeLocalTrees(
      localTrees: Seq[LocalTree],
      desiredCubeSize: Int,
      indexStatus: IndexStatus,
      isReplication: Boolean)(
      missingCubeDomain: (CubeId, LocalTree) => Double): Map[CubeId, NormalizedWeight] = {
    var globalCubeNormalizedWeights = Map.empty[CubeId, NormalizedWeight]

    // TODO This operation is very expensive. Find an alternative that allows a top-down \
    //  traversal of the trees, without missing any cube.
    val allCubes = SortedSet(localTrees.flatMap(_.keys): _*)
    val minLeafWeight = 1d

    allCubes.foreach(cube => {
      // When not replicating, we can skip the cube, and consequently its subtree, when
      // its immediate parent or any of its ancestor cubes was a leaf.
      val skipCube = !isReplication && !cube.isRoot && (globalCubeNormalizedWeights
        .getOrElse(cube.parent.get, minLeafWeight) >= minLeafWeight)

      if (!skipCube) {
        val parentGlobalWeight = cube.parent match {
          case None => 0d // The root has 0d as parentWeight.
          case Some(parent) =>
            if (globalCubeNormalizedWeights.contains(parent)) globalCubeNormalizedWeights(parent)
            // It is possible for a LocalTree to have some upper levels missing during optimization.
            // In that case, if the parent is not present, we take the parent weight from the
            // existing global index.
            else if (isReplication) indexStatus.cubesStatuses(parent).normalizedWeight
            else {
              throw new RuntimeException(s"Unable to find global parent weight for cube: $cube")
            }
        }

        var cubeTreeSize = 0d
        var cubeDomain = 0d
        for (tree <- localTrees) {
          // cube exists in the local tree, its cube domain is to be computed using its tree size.
          if (tree.contains(cube)) {
            val treeSize = tree(cube).treeSize
            val parentLocalWeight = cube.parent match {
              case None => 0d
              case Some(parent) =>
                if (tree.contains(parent)) tree(parent).normalizedWeight
                else if (isReplication) 0d
                else {
                  throw new RuntimeException(
                    s"Unable to find local parent weight for cube: $cube")
                }
            }
            cubeTreeSize += treeSize
            cubeDomain += treeSize / (1d - parentLocalWeight)
          } else {
            // The cube is missing from the local tree, estimate an upper-bound
            // for its domain from its chain of existing ancestors.
            cubeDomain += missingCubeDomain(cube, tree)
          }
        }

        // A cube is forced to be a leaf cube in the global index if its tree size is below
        // the desired cube size. This is a preventive procedure for small inner cubes:
        // When the domain estimation is too large with respect to the real value, which often
        // happens in the vicinity of leaf cubes whose real domain values are small and there's
        // a reduced margin for error, the algorithm outputs a much smaller weight leading to
        // small inner cubes.
        val normalizedWeight =
          if (!isReplication && cubeTreeSize <= 0.98 * desiredCubeSize) {
            NormalizedWeight.apply(desiredCubeSize, cubeTreeSize.toLong)
          } else parentGlobalWeight + (desiredCubeSize / cubeDomain)

        globalCubeNormalizedWeights += (cube -> normalizedWeight)
      }
    })

    globalCubeNormalizedWeights
  }

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    // Compute the statistics for the indexedColumns
    val columnTransformers = indexStatus.revision.columnTransformers
    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)

    val numElements = dataFrameStats.getAs[Long]("count")
    if (numElements == 0) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    val spaceChanges =
      if (isReplication) None
      else calculateRevisionChanges(dataFrameStats, indexStatus.revision)

    // The revision to use
    val revision = spaceChanges match {
      case Some(revisionChange) =>
        revisionChange.createNewRevision
      case None => indexStatus.revision
    }

    // Three step transformation

    // First, add a random weight column
    val weightedDataFrame = dataFrame.transform(addRandomWeight(revision))

    // Second, construct local trees with partition data
    val localTrees =
      weightedDataFrame
        .transform(computeLocalTrees(numElements, revision, indexStatus, isReplication))
        .collect()

    // Third, compute the overall estimated cube weights by merging local trees
    val estimatedCubeWeights =
      mergeLocalTrees(localTrees, revision.desiredCubeSize, indexStatus, isReplication)(
        MissingCubeDomainEstimation.domainThroughPayloadFractions)

    // Gather the new changes
    val tableChanges = BroadcastedTableChanges(
      spaceChanges,
      indexStatus,
      estimatedCubeWeights,
      if (isReplication) indexStatus.cubesToOptimize
      else Set.empty[CubeId])

    (weightedDataFrame, tableChanges)
  }

}
