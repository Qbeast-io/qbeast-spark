/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.index.QbeastColumns.{
  cubeColumnName,
  cubeToReplicateColumnName,
  stateColumnName,
  weightColumnName
}
import io.qbeast.spark.model._
import io.qbeast.spark.sql.qbeast.QbeastSnapshot
import io.qbeast.spark.sql.rules.Functions.qbeastHash
import io.qbeast.spark.sql.utils.State._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisExceptionFactory, Column, DataFrame, SparkSession}

import scala.collection.immutable.IndexedSeq

/**
 * Qbeast OTree algorithm for indexing give data frames.
 */
trait OTreeAlgorithm {

  /**
   * Indexes the first data frame of a new index being created
   *
   * @param dataFrame the data frame to index
   * @param columnsToIndex the columns to index
   * @return the indexed data frame, the space revision and the weightMap
   */
  def indexFirst(
      dataFrame: DataFrame,
      columnsToIndex: Seq[String]): (DataFrame, Revision, Map[CubeId, Weight])

  /**
   * Indexes a given non-first data frame using the current snapshot
   * of the index state and the announced set.
   *
   * @param dataFrame the data frame to append
   * @param snapshot the index state snapshot
   * @param announcedSet the announced set
   * @return the indexed data frame, the space revision and the weightMap
   */
  def indexNext(
      dataFrame: DataFrame,
      snapshot: QbeastSnapshot,
      announcedSet: Set[CubeId]): (DataFrame, Revision, Map[CubeId, Weight])

  /**
   * Returns the columns contributing to the pseudo random weight generation.
   *
   * @param schema the schema
   * @param columnsToIndex the columns to index
   * @return the columns
   */
  def getWeightContributorColumns(schema: StructType, columnsToIndex: Seq[String]): Seq[String]

  /**
   * The desired size of the cube.
   *
   * @return the desired size of the cube
   */
  def desiredCubeSize: Int

  /**
   * Takes the data from different cubes and replicates it to their children
   *
   * @param dataFrame data to be replicated
   * @param spaceRevision current space revision to index
   * @param qbeastSnapshot current snapshot of the index
   * @param cubesToReplicate set of cubes to replicate
   * @return the modified dataFrame with replicated data
   */
  def replicateCubes(
      dataFrame: DataFrame,
      spaceRevision: Revision,
      qbeastSnapshot: QbeastSnapshot,
      cubesToReplicate: Set[CubeId]): (DataFrame, Map[CubeId, Weight])

  /**
   * Analyze the index structure and returns which cubes need to be optimized
   *
   * @param qbeastSnapshot snapshot
   * @param spaceRevision space revision to review
   * @return the sequence of cubes that need optimization
   */
  def analyzeIndex(qbeastSnapshot: QbeastSnapshot, spaceRevision: Revision): Seq[CubeId]

}

/**
 * Implementation of OTreeAlgorithm.
 *
 * @param desiredCubeSize the desired size of the cube
 */
final class OTreeAlgorithmImpl(val desiredCubeSize: Int)
    extends OTreeAlgorithm
    with Serializable {

  /**
   * Estimates MaxWeight on DataFrame
   */
  val maxWeightEstimation = udaf(MaxWeightEstimation)

  override def indexFirst(
      dataFrame: DataFrame,
      columnsToIndex: Seq[String]): (DataFrame, Revision, Map[CubeId, Weight]) = {
    // splitting the list of columns in two to fit the signature of the agg method.
    val spaceRevision = Revision(dataFrame, columnsToIndex, desiredCubeSize)
    val (indexedDataFrame, cubeWeights: Map[CubeId, Weight]) = index(
      dataFrame = dataFrame,
      spaceRevision = spaceRevision,
      cubeNormalizedWeights = Map.empty,
      announcedSet = Set.empty,
      replicatedSet = Set.empty,
      isReplication = false)
    (indexedDataFrame, spaceRevision, cubeWeights)
  }

  override def indexNext(
      dataFrame: DataFrame,
      snapshot: QbeastSnapshot,
      announcedSet: Set[CubeId]): (DataFrame, Revision, Map[CubeId, Weight]) = {
    val spaceRevision = snapshot.lastSpaceRevision
    val (indexedDataFrame, cubeWeights: Map[CubeId, Weight]) = index(
      dataFrame = dataFrame,
      spaceRevision,
      cubeNormalizedWeights = snapshot.cubeNormalizedWeights(spaceRevision),
      announcedSet = announcedSet,
      replicatedSet = snapshot.replicatedSet(spaceRevision),
      isReplication = false)
    (indexedDataFrame, spaceRevision, cubeWeights)
  }

  override def getWeightContributorColumns(
      schema: StructType,
      columnsToIndex: Seq[String]): Seq[String] = {
    val fields = columnsToIndex.map(column => schema.find(_.name == column).get)
    val notNulls =
      fields.filter(p => !p.nullable)
    if (notNulls.isEmpty) {
      fields
    } else {
      notNulls
    }
  }.map(_.name)

  override def analyzeIndex(
      qbeastSnapshot: QbeastSnapshot,
      spaceRevision: Revision): Seq[CubeId] = {

    val dimensionCount = qbeastSnapshot.indexedCols.length
    val overflowedSet = qbeastSnapshot.overflowedSet(spaceRevision)
    val replicatedSet = qbeastSnapshot.replicatedSet(spaceRevision)

    val cubesToOptimize = overflowedSet
      .filter(cube => {
        !replicatedSet.contains(cube) && (cube.parent match {
          case None => true
          case Some(p) => replicatedSet.contains(p)
        })
      })

    if (cubesToOptimize.isEmpty && replicatedSet.isEmpty) {
      Seq(CubeId.root(dimensionCount))
    } else cubesToOptimize.toSeq
  }

  override def replicateCubes(
      dataFrame: DataFrame,
      spaceRevision: Revision,
      qbeastSnapshot: QbeastSnapshot,
      announcedSet: Set[CubeId]): (DataFrame, Map[CubeId, Weight]) = {

    val cubeWeights = qbeastSnapshot.cubeNormalizedWeights(spaceRevision)
    val replicatedSet = qbeastSnapshot.replicatedSet(spaceRevision)

    index(
      dataFrame = dataFrame,
      spaceRevision = spaceRevision,
      cubeNormalizedWeights = cubeWeights,
      announcedSet = announcedSet,
      replicatedSet = replicatedSet,
      isReplication = true)

  }

  private def index(
      dataFrame: DataFrame,
      spaceRevision: Revision,
      cubeNormalizedWeights: Map[CubeId, NormalizedWeight],
      announcedSet: Set[CubeId],
      replicatedSet: Set[CubeId],
      isReplication: Boolean): (DataFrame, Map[CubeId, Weight]) = {

    val sqlContext = SparkSession.active.sqlContext
    import sqlContext.implicits._

    val dimensionColumns = spaceRevision.dimensionColumns
    val weightedDataFrame =
      dataFrame.transform(df => addRandomWeight(df, dimensionColumns))

    val partitionCount = weightedDataFrame.rdd.getNumPartitions

    val partitionedDesiredCubeSize = if (partitionCount > 0) {
      desiredCubeSize / partitionCount
    } else {
      desiredCubeSize
    }

    val dimensionCount = spaceRevision.dimensionCount
    val selectionColumns =
      if (isReplication) dimensionColumns ++ Seq(weightColumnName, cubeToReplicateColumnName)
      else dimensionColumns ++ Seq(weightColumnName)

    val partitionedEstimatedCubeWeights = weightedDataFrame
      .selectExpr(selectionColumns: _*)
      .mapPartitions(rows => {
        val weights =
          new CubeWeightsBuilder(
            partitionedDesiredCubeSize,
            partitionCount,
            announcedSet,
            replicatedSet)
        rows.foreach { row =>
          val values = dimensionColumns.map(row.getAs[Any])
          val point = rowValuesToPoint(values, spaceRevision)
          val weight = Weight(row.getAs[Int](weightColumnName))
          if (isReplication) {
            val parentBytes = row.getAs[Array[Byte]](cubeToReplicateColumnName)
            val parent = Some(CubeId(dimensionCount, parentBytes))
            weights.update(point, weight, parent)
          } else weights.update(point, weight)
        }
        weights.result().iterator
      })

    // These column names are the ones specified in case clas CubeNormalizedWeight
    val estimatedCubeWeights = partitionedEstimatedCubeWeights
      .groupBy("cubeBytes")
      .agg(maxWeightEstimation(col("normalizedWeight")))
      .collect()
      .map { row =>
        val bytes = row.getAs[Array[Byte]](0)
        val estimatedWeight = row.getAs[Double](1)
        (CubeId(dimensionCount, bytes), estimatedWeight)
      }
      .toMap

    val mergedCubeWeights = CubeWeights.merge(cubeNormalizedWeights, estimatedCubeWeights)
    val weightsMessage = sqlContext.sparkContext.broadcast(mergedCubeWeights)

    val findTargetCubeIds =
      udf((rowValues: Seq[Any], weightValue: Int, parentBytes: Any) => {
        val point = rowValuesToPoint(rowValues, spaceRevision)
        val weight = Weight(weightValue)
        val parent = parentBytes match {
          case bytes: Array[Byte] => Some(CubeId(dimensionCount, bytes))
          case _ => None
        }
        CubeWeights
          .findTargetCubeIds(
            point,
            weight,
            weightsMessage.value,
            announcedSet,
            replicatedSet,
            parent)
          .map(_.bytes)
          .toArray
      })

    val indexedDataFrame = weightedDataFrame
      .withColumn(
        cubeColumnName,
        explode(
          findTargetCubeIds(
            rowValuesColumn(dimensionColumns),
            col(weightColumnName), {
              if (isReplication) col(cubeToReplicateColumnName)
              else lit(null)
            })))
      .transform(extendWithType(dimensionCount, announcedSet, replicatedSet))
      .drop(cubeToReplicateColumnName)

    (indexedDataFrame, mergedCubeWeights)
  }

  private def extendWithType(
      dimensionCount: Int,
      announcedSet: Set[CubeId],
      replicatedSet: Set[CubeId]): DataFrame => DataFrame = df => {

    val states = udf { (bytes: Array[Byte]) =>
      val cubeId = CubeId(dimensionCount, bytes)
      if (announcedSet.contains(cubeId) && !replicatedSet.contains(cubeId)) {
        ANNOUNCED
      } else if (replicatedSet.contains(cubeId)) {
        REPLICATED
      } else {
        FLOODED
      }
    }

    df.withColumn(stateColumnName, states(col(cubeColumnName)))

  }

  private def rowValuesToPoint(values: Seq[Any], spaceRevision: Revision): Point = {
    val coordinates = IndexedSeq.newBuilder[Double]
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

  private def addRandomWeight(df: DataFrame, columnsToIndex: Seq[String]): DataFrame = {
    val columns = getWeightContributorColumns(df.schema, columnsToIndex).map(name => df(name))
    df.withColumn(weightColumnName, qbeastHash(columns: _*))
  }

}
