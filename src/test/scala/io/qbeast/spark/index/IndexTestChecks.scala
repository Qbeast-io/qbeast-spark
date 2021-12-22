package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, Revision, TableChanges, Weight}
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.spark.sql.DataFrame
import org.scalatest.AppendedClues.convertToClueful
import org.scalatest.matchers.should.Matchers

trait IndexTestChecks extends Matchers {

  def checkDFSize(indexed: DataFrame, original: DataFrame): Unit = {
    val indexedSize = indexed.count()
    val originalSize = original.count()
    indexedSize shouldBe originalSize withClue
      s"Indexed dataset has size $indexedSize and original has size $originalSize"
  }

  def checkCubeSize(tableChanges: TableChanges, revision: Revision, indexed: DataFrame): Unit = {
    val weightMap: Map[CubeId, Weight] = tableChanges.cubeWeights
    val desiredCubeSize = revision.desiredCubeSize
    val minSize = (desiredCubeSize * 0.9).toLong

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
            // then the size should be greater than the desiredCubeSize
            (size should be > minSize) withClue
              s"cube ${cubeId.string} appear as overflowed but has size $size"

            // And parent cube should be overflowed as well
            cubeId.parent match {
              case None => // cube is root

              case Some(parent) if weightMap.contains(parent) && cubeSizes.contains(parent) =>
                val weightParent = weightMap(parent)
                val parentSize = cubeSizes(parent)
                weightParent should not be Weight.MaxValue

                (size should be > minSize) withClue
                  s"cube $cubeId is overflowed but parent ${parent.string} is not" +
                  s" It has weight $weightParent and size $parentSize"

              case Some(parent) =>
                fail(
                  s"Parent ${parent.string} of ${cubeId.string}" +
                    s" does not appear in weight map or data")

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
          (weightMap should contain key parent) withClue
            s"parent ${parent.string} of ${cube.string} does not appear in the list of cubes"
        case None => // root cube
      }
    }
  }

  def checkCubesOnData(
      weightMap: Map[CubeId, Weight],
      indexed: DataFrame,
      dimensionCount: Int): Unit = {

    val cubesOnData = indexed
      .select(cubeColumnName)
      .distinct()
      .collect()
      .map(row => CubeId(dimensionCount, row.getAs[Array[Byte]](cubeColumnName)))

    def checkEmptyParents(): Unit = {
      cubesOnData.foreach { cube =>
        cube.parent match {
          case Some(parent) =>
            (cubesOnData should contain(parent)) withClue
              s"Parent ${parent.string} of ${cube.string} does not appear in the indexed data"

          case None => // root cube
        }
      }
    }

    def checkDataWithWeightMap(): Unit = {
      cubesOnData.foreach { cube =>
        if (cube.isRoot) {

          (weightMap should contain key cube) withClue
            s"Cube root appears in data but not in weight map"
        } else {
          val parent = cube.parent.get

          (weightMap should (contain key cube or contain key parent)) withClue
            s"Either weight map doesn't contain ${cube.string}" +
            s" or doesn't contain it's parent ${parent.string}"
        }
      }
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
      // scalastyle:off
      childrenWeights.foreach { case (child, childWeight) =>
        childWeight should be >= maxWeight withClue
          s"MaxWeight of child ${child.string} is ${childWeight.fraction} " +
          s"and maxWeight of parent ${cube.string} is ${maxWeight.fraction}"
      }
    }
  }

}
