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

import io.qbeast.core.model.PointWeightIndexer
import io.qbeast.core.model.TableChanges
import io.qbeast.core.model.Weight
import io.qbeast.spark.index.QbeastColumns._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

private class SparkPointWeightIndexer(tableChanges: TableChanges, isReplication: Boolean)
    extends Serializable {

  private val revision = tableChanges.updatedRevision
  private val pointIndexer: PointWeightIndexer = PointWeightIndexer(tableChanges)

  private val findTargetCubeIdsUDF: UserDefinedFunction = {

    udf((rowValues: Row, weightValue: Int) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      pointIndexer
        .findTargetCubeIds(point, weight)
        .map(_.bytes)
        .toArray
    })
  }

  private val replicateTargetCubeIds: UserDefinedFunction = {

    udf((rowValues: Row, weightValue: Int, parentBytes: Array[Byte]) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      val parent = revision.createCubeId(parentBytes)
      pointIndexer
        .findTargetCubeIds(point, weight, Some(parent))
        .map(_.bytes)
        .toArray
    })
  }

  def buildIndex: DataFrame => DataFrame = (weightedDataFrame: DataFrame) => {

    val revision = tableChanges.updatedRevision
    val columnsToIndex = revision.columnTransformers.map(_.columnName)
    if (isReplication) {
      weightedDataFrame
        .withColumn(
          cubeColumnName,
          explode(
            replicateTargetCubeIds(
              struct(columnsToIndex.map(col): _*),
              col(weightColumnName),
              col(cubeToReplicateColumnName))))
    } else {
      weightedDataFrame
        .withColumn(
          cubeColumnName,
          explode(
            findTargetCubeIdsUDF(struct(columnsToIndex.map(col): _*), col(weightColumnName))))
    }

  }

}
