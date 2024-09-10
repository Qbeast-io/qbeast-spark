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
package io.qbeast.spark.delta.index

import io.qbeast.core.model.{CubeId, Weight}
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

    def checkDataWithWeightMap(): Unit = {
      cubesOnData.foreach { cube =>
        if (cube.isRoot) {

          (weightMap should contain key cube) withClue
            "Cube root appears in data but not in weight map"
        } else {
          val parent = cube.parent.get

          (weightMap should (contain key cube or contain key parent)) withClue
            s"Either weight map doesn't contain ${cube.string}" +
            s" or doesn't contain it's parent ${parent.string}"
        }
      }
    }

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
