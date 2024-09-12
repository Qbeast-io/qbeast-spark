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
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.spark.snapshot.SparkQbeastSnapshot
import io.qbeast.IISeq
import io.qbeast.core.index.query.QuerySpec
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.Dataset

import java.net.URI
import scala.collection.mutable

/**
 * Executes a query against a Qbeast snapshot
 * @param querySpecBuilder
 *   the builder for the query specification
 */
class QueryExecutor(querySpecBuilder: QuerySpecBuilder, qbeastSnapshot: SparkQbeastSnapshot) {

  /**
   * Executes the query on each revision according to their QuerySpec
   * @return
   *   the final sequence of blocks that match the query
   */
  def execute(tablePath: Path): Seq[FileStatusWithMetadata] = {

    qbeastSnapshot.loadAllRevisions
      .filter(_.revisionID > 0)
      .flatMap { revision =>
        val indexFiles: Dataset[IndexFile] = qbeastSnapshot.loadIndexFiles(revision.revisionID)
        import indexFiles.sparkSession.implicits._
        val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
        val querySpecs = querySpecBuilder.build(revision)
        querySpecs
          .map { querySpec =>
            {
              val cubesList = (querySpec.isSampling, querySpec.querySpace) match {
                case (_, _: QuerySpaceFromTo) | (true, _: AllSpace) =>
                  executeRevision(querySpec, indexStatus)
                case (false, _: AllSpace) =>
                  indexStatus.cubesStatuses.keys.toSeq
                case _ => Seq.empty[CubeId]
              }
              val cubes = cubesList
                .toDS()
                .distinct()
                .select(struct(col("*")).as("cubeId"))

              indexFiles
                .select(struct(col("*")).as("indexFile"), explode(col("blocks")))
                .select("indexFile", "col.*")
                .join(cubes, "cubeId")
                .where(lit(querySpec.weightRange.from.value) <= col("maxWeight.value")
                  && lit(querySpec.weightRange.to.value) > col("minWeight.value"))
                .select(
                  col("indexFile.path"),
                  col("indexFile.size"),
                  col("indexFile.modificationTime"))
                .distinct()
                .as[(String, Long, Long)]

            }
          }
      }
      .flatMap(_.collect())
      .map { case (filePath: String, size: Long, modificationTime: Long) =>
        var path = new Path(new URI(filePath))
        if (!path.isAbsolute) {
          path = new Path(tablePath, path)
        }
        FileStatusWithMetadata(new FileStatus(size, false, 0, 0, modificationTime, path))

      }
  }

  private[query] def executeRevision(
      querySpec: QuerySpec,
      indexStatus: IndexStatus): IISeq[CubeId] = {

    val outputCubeIds = Vector.newBuilder[CubeId]
    val stack = mutable.Stack(indexStatus.revision.createCubeIdRoot())
    while (stack.nonEmpty) {
      val currentCube = stack.pop()

      val cubeIter = indexStatus.cubesStatuses.iteratorFrom(currentCube)
      // Contains cases for the next element from the iterator being
      // 1. the cube itself
      // 2. one of the cube's children
      // 3. this currentCube's sibling or their subtree
      // 4. empty, the currentCube is the right-most cube in the tree and it is not in cubesStatuses
      if (cubeIter.hasNext) { // cases 1 to 3
        cubeIter.next() match {
          case (cube, CubeStatus(_, maxWeight, _, _, _)) if cube == currentCube => // Case 1
            if (querySpec.weightRange.to <= maxWeight) {
              // cube maxWeight is larger than or equal to the sample fraction (weightRange.to),
              // that currentCube is the last cube to visit from the current branch - all blocks
              // are to be retrieved and no more cubes from the branch should be visited.
              outputCubeIds += cube
            } else {
              // Otherwise,
              // 1. if the currentCube is REPLICATED, we skip the cube
              // 2. if the state is ANNOUNCED, ignore the After Announcement elements
              // 3. if FLOODED, retrieve all files from the cube
              val isReplicated = indexStatus.replicatedSet.contains(cube)

              if (!isReplicated) {
                outputCubeIds += cube
              }
              val nextLevel = cube.children
                .filter(querySpec.querySpace.intersectsWith)
              stack.pushAll(nextLevel)

            }

          case (cube, _) if currentCube.isAncestorOf(cube) => // Case 2
            // cube is a descendant of currentCube, and currentCube is missing.
            // We proceed navigating the subtree.
            val nextLevel = currentCube.children
              .filter(querySpec.querySpace.intersectsWith)
            stack.pushAll(nextLevel)

          case _ => // Case 3
        }
      }
    }
    outputCubeIds.result()
  }

}
