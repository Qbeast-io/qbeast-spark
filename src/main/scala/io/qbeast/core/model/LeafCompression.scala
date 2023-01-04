package io.qbeast.core.model

import scala.collection.mutable

object LeafCompression {

  def compress(
      cubeNormalizedWeights: Map[CubeId, NormalizedWeight],
      announcedOrReplicatedSet: Set[CubeId],
      desiredCubeSize: Int): Set[CubeId] = {
    if (cubeNormalizedWeights.isEmpty ||
      (cubeNormalizedWeights.size == 1 && cubeNormalizedWeights.keys.head.isRoot)) {
      Set.empty[CubeId]
    } else {
      val compressedLeaves = mutable.Seq.newBuilder[CubeId]
      compressedLeaves.sizeHint(cubeNormalizedWeights.size)

      cubeNormalizedWeights
        .filterNot(cw =>
          cw._1.children.exists(cubeNormalizedWeights.contains) ||
            announcedOrReplicatedSet.contains(cw._1))
        .mapValues(desiredCubeSize / _)
        .groupBy(_._1.parent.get)
        .filterNot(pCw => announcedOrReplicatedSet.contains(pCw._1))
        .foreach { case (_, leafSizes) =>
          val leafSizeIter = leafSizes.toSeq.sortBy(_._2).toIterator
          var continue = true
          var accSize = 0L
          while (continue && leafSizeIter.hasNext) {
            val (leaf, leafSize) = leafSizeIter.next()
            if (accSize + leafSize <= desiredCubeSize) {
              accSize += leafSize.toInt
              compressedLeaves += leaf
            } else {
              continue = false
            }
          }
        }

      compressedLeaves.result().toSet
    }
  }

}
