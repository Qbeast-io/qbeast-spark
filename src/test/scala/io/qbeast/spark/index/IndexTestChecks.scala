package io.qbeast.spark.index

import io.qbeast.model.{CubeId, Revision, TableChanges, Weight}
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.spark.sql.DataFrame

object IndexTestChecks {

  def checkCubeSize(tableChanges: TableChanges, revision: Revision, indexed: DataFrame): Unit = {
    val weightMap: Map[CubeId, Weight] = tableChanges.indexChanges.cubeWeights
    val desiredCubeSize = revision.desiredCubeSize
    val cubeSizes = indexed
      .groupBy(cubeColumnName)
      .count()
      .collect()
      .map(row =>
        (revision.createCubeId(row.getAs[Array[Byte]](cubeColumnName)), row.getAs[Long]("count")))
      .toMap

    cubeSizes.foreach { case (cubeId: CubeId, size: Long) =>
      weightMap.get(cubeId) match {
        case Some(weight) =>
          if (weight != Weight.MaxValue) {
            // If the weight is not set to MaxValue,
            // then the size should be greater than more or less the desiredCubeSize
            assert(
              size > desiredCubeSize * 0.9,
              s"cube ${cubeId.string} appear as overflowed but has size $size")

            // And parent cube should be overflowed as well
            cubeId.parent match {
              case None => // cube is root
              case Some(parent) =>
                assert(
                  cubeSizes(parent) > desiredCubeSize * 0.9,
                  s"cube ${cubeId.string} is overflowed but parent ${parent.string} is not")
            }
          }
        case None =>
      }
    }
  }

  def checkCubes(weightMap: Map[CubeId, Weight]): Unit = {

    weightMap.foreach { case (cube, _) =>
      cube.parent match {
        case Some(parent) =>
          assert(
            weightMap.contains(parent),
            s"parent ${parent.string} of ${cube.string} does not appear in the list of cubes")
        case None => // root cube
      }
    }
  }

  def checkCubesOnData(
      weightMap: Map[CubeId, Weight],
      indexed: DataFrame,
      dimensionCount: Int): Unit = {

    val cubes = indexed
      .select(cubeColumnName)
      .distinct()
      .collect()
      .map(row => CubeId(dimensionCount, row.getAs[Array[Byte]](cubeColumnName)))

    def checkEmptyParents(): Unit = {
      cubes.foreach { case cube =>
        cube.parent match {
          case Some(parent) =>
            assert(
              cubes.contains(parent),
              s"parent ${parent.string} of ${cube.string} does not appear in the indexed data")
          case None => // root cube
        }
      }
    }

    def checkDataWithWeightMap(): Unit = {
      cubes.foreach(cube =>
        assert(weightMap.contains(cube) || weightMap.contains(cube.parent.get)))
    }

    checkEmptyParents()
    checkDataWithWeightMap()
  }

  def checkWeightsIncrement(weightMap: Map[CubeId, Weight]): Unit = {

    weightMap.foreach { case (cube: CubeId, maxWeight: Weight) =>
      val children = cube.children.toSet
      val childrenWeights = weightMap.filter { case (candidate, _) =>
        children.contains(candidate)
      }
      childrenWeights.foreach { case (child, childWeight) =>
        assert(
          childWeight >= maxWeight,
          s"MaxWeight of child ${child.string} is ${childWeight.fraction} " +
            s"and maxWeight of parent ${cube.string} is ${maxWeight.fraction}")
      }
    }
  }

}
