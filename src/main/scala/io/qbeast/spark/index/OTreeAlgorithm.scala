/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model._
import io.qbeast.spark.index.QbeastColumns.{
  cubeColumnName,
  cubeToReplicateColumnName,
  stateColumnName,
  weightColumnName
}
import io.qbeast.spark.utils.State
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisExceptionFactory, Column, DataFrame, SparkSession}

/**
 * Qbeast OTree algorithm for indexing give data frames.
 */
trait OTreeAlgorithm {

  /**
   * Indexes the first data frame of a new index being created
   *
   * @param dataFrame the data frame to index
   * @param revisionData the revision data
   * @return the indexed data frame, the space revision and the weightMap
   */
  def index(
      dataFrame: DataFrame,
      revisionData: IndexStatus,
      isReplication: Boolean = false): (DataFrame, TableChanges)

  /**
   * Takes the data from different cubes and replicates it to their children
   *
   * @param dataFrame data to be replicated
   * @param revisionData current revision snapshot to index
   * @param cubesToReplicate the cubes that we are going to replicate
   * @return the modified dataFrame with replicated data
   */
  def replicateCubes(dataFrame: DataFrame, revisionData: IndexStatus): (DataFrame, TableChanges)

  /**
   * Analyze the index structure and returns which cubes need to be optimized
   *
   * @param revisionData data of a single Revision
   * @return the sequence of cubes that need optimization
   */
  def analyzeIndex(revisionData: IndexStatus): Seq[CubeId]

}

/**
 * Implementation of OTreeAlgorithm.
 */
object OTreeAlgorithmImpl extends OTreeAlgorithm with Serializable {

  override def analyzeIndex(revisionData: IndexStatus): Seq[CubeId] = {

    val overflowedSet = revisionData.overflowedSet
    val replicatedSet = revisionData.replicatedSet

    val cubesToOptimize = overflowedSet
      .filter(cube => {
        !replicatedSet.contains(cube) && (cube.parent match {
          case None => true
          case Some(p) => replicatedSet.contains(p)
        })
      })

    if (cubesToOptimize.isEmpty && replicatedSet.isEmpty) {
      Seq(revisionData.revision.createCubeIdRoot())
    } else cubesToOptimize.toSeq
  }

  override def replicateCubes(
      dataToReplicate: DataFrame,
      revisionData: IndexStatus): (DataFrame, TableChanges) = {

    index(dataFrame = dataToReplicate, revisionData = revisionData, isReplication = true)

  }

  override def index(
      dataFrame: DataFrame,
      revisionData: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    val sqlContext = SparkSession.active.sqlContext

    val (weightedDataFrame, tc) =
      DoublePassOTreeDataAnalyzer.analyze(dataFrame, revisionData, isReplication)
    val revision = tc.updatedRevision

    val pointWeightIndexer = PointWeightIndexer.buildNewWeightIndexer(tc.indexChanges)

    val pwiBC = sqlContext.sparkContext.broadcast(pointWeightIndexer)
    val findTargetCubeIds =
      udf((rowValues: Seq[Any], weightValue: Int, parentBytes: Any) => {
        val point = rowValuesToPoint(rowValues, revision)
        val weight = Weight(weightValue)
        val parent = parentBytes match {
          case bytes: Array[Byte] => Some(revision.createCubeId(bytes))
          case _ => None
        }
        pwiBC.value
          .findTargetCubeIds(point, weight, parent)
          .map(_.bytes)
          .toArray
      })

    val columnsToIndex = revision.columnTransformers.map(_.columnName)
    val indexedDataFrame = weightedDataFrame
      .withColumn(
        cubeColumnName,
        explode(
          findTargetCubeIds(
            rowValuesColumn(columnsToIndex),
            col(weightColumnName), {
              if (isReplication) col(cubeToReplicateColumnName)
              else lit(null)
            })))
      .transform(
        extendWithType(
          columnsToIndex.length,
          tc.indexChanges.announcedSet,
          revisionData.replicatedSet))
      .drop(cubeToReplicateColumnName)

    (indexedDataFrame, tc)
  }

  private def extendWithType(
      dimensionCount: Int,
      announcedSet: Set[CubeId],
      replicatedSet: Set[CubeId]): DataFrame => DataFrame = df => {

    val states = udf { (bytes: Array[Byte]) =>
      val cubeId = CubeId(dimensionCount, bytes)
      if (announcedSet.contains(cubeId) && !replicatedSet.contains(cubeId)) {
        State.ANNOUNCED
      } else if (replicatedSet.contains(cubeId)) {
        State.REPLICATED
      } else {
        State.FLOODED
      }
    }

    df.withColumn(stateColumnName, states(col(cubeColumnName)))

  }

  def rowValuesToPoint(values: Seq[Any], spaceRevision: Revision): Point = Point {
    val coordinates = Vector.newBuilder[Double]
    coordinates.sizeHint(values.length)
    for (value <- values) {
      value match {
        case n: Number =>
          coordinates += n.doubleValue()
        case null =>
          throw AnalysisExceptionFactory.create(
            "Column to index contains null values. Please initialize them before indexing")
        case _ =>
          throw AnalysisExceptionFactory.create("Column to index contains non-numeric value")
      }
    }
    spaceRevision.transform(coordinates.result())
  }

  private def rowValuesColumn(columnsToIndex: Seq[String]): Column =
    array(columnsToIndex.map(col): _*)

}
