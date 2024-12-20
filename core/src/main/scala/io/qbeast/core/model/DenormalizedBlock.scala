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
package io.qbeast.core.model

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedSet

/**
 * * This utility case class represent a block, with all the related denormalized information.
 *
 * @param cubeId
 *   The identifier of the cube the block belongs to
 * @param filePath
 *   The path of the file the block belongs to
 * @param revisionId
 *   The revision number
 * @param fileSize
 *   The size of the file, in bytes and including other blocks.
 * @param fileModificationTime
 *   The last modification time of the file
 * @param minWeight
 *   The block minimum weight
 * @param maxWeight
 *   The block maximum weight
 * @param blockElementCount
 *   The number of elements in the block
 */
case class DenormalizedBlock(
    cubeId: CubeId,
    isLeaf: Boolean,
    filePath: String,
    revisionId: RevisionID,
    fileSize: Long,
    fileModificationTime: Long,
    minWeight: Weight,
    maxWeight: Weight,
    blockElementCount: Long)

private[qbeast] object DenormalizedBlock {

  private[qbeast] def isLeaf(cubeIds: SortedSet[CubeId])(cubeId: CubeId): Boolean = {
    // cubeStatuses are stored in a SortedMap with CubeIds ordered as if they were accessed
    // in a pre-order, DFS fashion.
    val cubesIter = cubeIds.iteratorFrom(cubeId)
    cubesIter.take(2).toList match {
      case List(cube, nextCube) =>
        // cubeId is in the tree and check the next cube
        if (cube == cubeId) !cubeId.isAncestorOf(nextCube)
        // cubeId is not in the tree, check the cube after it
        else !cubeId.isAncestorOf(cube)
      case List(cube) =>
        // only one cube is larger than or equal to cubeId, and it is the cubeId itself
        if (cube == cubeId) true
        // cubeId is not in the map, check the cube after it
        else !cubeId.isAncestorOf(cube)
      case Nil =>
        // cubeId is the smaller than any existing cube and does not belong to the map
        true
    }
  }

  def buildDataset(indexFilesDS: Dataset[IndexFile]): Dataset[DenormalizedBlock] = {
    val spark = indexFilesDS.sparkSession
    import spark.implicits._
    val cubeIdsBuilder = SortedSet.newBuilder[CubeId]
    indexFilesDS
      .select(explode(col("blocks")).as("block"))
      .select("block.cubeId.*")
      .distinct()
      .as[CubeId]
      .collect()
      .foreach(cubeId => cubeIdsBuilder += cubeId)
    val cubeIds = spark.sparkContext.broadcast(cubeIdsBuilder.result())
    val isLeafUDF: UserDefinedFunction =
      udf((cubeId: CubeId) => DenormalizedBlock.isLeaf(cubeIds.value)(cubeId))
    indexFilesDS
      .withColumn("block", explode(col("blocks")))
      .select(
        $"block.cubeId".as("cubeId"),
        isLeafUDF($"block.cubeId").as("isLeaf"),
        $"path".as("filePath"),
        $"revisionID",
        $"size".as("fileSize"),
        $"modificationTime".as("fileModificationTime"),
        $"block.minWeight".as("minWeight"),
        $"block.maxWeight".as("maxWeight"),
        $"block.elementCount".as("blockElementCount"))
      .as[DenormalizedBlock]
  }

}
