package io.qbeast.core.model

import io.qbeast.IISeq

import scala.collection.mutable

/**
 * Query Manager template
 *
 * @tparam QUERY type of the query
 */
trait QueryManager[QUERY] {

  /**
   * Executes a query against a index
   * @param query the query
   * @param qbeastSnapshot the current snapshot of the index
   * @return the result of the query
   */
  def query(query: QUERY, qbeastSnapshot: QbeastSnapshot): IISeq[QbeastBlock] = {
    qbeastSnapshot.loadAllRevisions.flatMap { revision =>
      val querySpec = buildSpec(query, revision)
      (querySpec.isSampling, querySpec.querySpace) match {
        case (_, _: QuerySpaceFromTo) | (true, _: AllSpace) =>
          val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
          val matchingBlocks = queryRevision(querySpec, indexStatus)
          matchingBlocks
        case (false, _: AllSpace) =>
          val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
          indexStatus.cubesStatuses.values.flatMap { status =>
            status.files.filter(_.state == State.FLOODED)
          }
        case _ => Seq.empty[QbeastBlock]
      }
    }
  }

  def queryRevision(querySpec: QuerySpec, indexStatus: IndexStatus): IISeq[QbeastBlock] = {
    val outputFiles = Vector.newBuilder[QbeastBlock]
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
          case (cube, CubeStatus(_, maxWeight, _, files)) if cube == currentCube => // Case 1
            val unfilteredFiles = if (querySpec.weightRange.to < maxWeight) {
              // cube maxWeight is larger than the sample fraction, weightRange.to,
              // it means that currentCube is the last cube to visit from the current branch.
              // All files are retrieved and no more cubes from the branch will be visited.
              files
            } else {
              // Otherwise,
              // 1. if the currentCube is REPLICATED, we skip the cube
              // 2. if the state is ANNOUNCED, ignore the After Announcement elements
              // 3. if FLOODED, retrieve all files from the cube
              val isReplicated = indexStatus.replicatedSet.contains(cube)
              val isAnnounced = indexStatus.announcedSet.contains(cube)
              val cubeFiles =
                if (isReplicated) {
                  Vector.empty
                } else if (isAnnounced) {
                  files.filterNot(_.state == State.ANNOUNCED)
                } else {
                  files
                }
              val nextLevel = cube.children
                .filter(querySpec.querySpace.intersectsWith)
              stack.pushAll(nextLevel)
              cubeFiles
            }

            outputFiles ++= unfilteredFiles.filter(file =>
              file.maxWeight > querySpec.weightRange.from)

          case (cube, _) if currentCube.isAncestorOf(cube) => // Case 2
            // c is a child cube of currentCube. Aside from c, we also need to
            // consider c's sibling cubes.
            val nextLevel = currentCube.children
              .filter(querySpec.querySpace.intersectsWith)
            stack.pushAll(nextLevel)

          case _ => // Case 3
        }
      }
    }
    outputFiles.result()
  }

  /**
   * Builds a QuerySpec for a specific revision
   * @param query
   * @param revision
   * @return
   */
  def buildSpec(query: QUERY, revision: Revision): QuerySpec

}
