package io.qbeast.core.model

import scala.collection.mutable

object LeafCompression {

  def compress(
      cubeNormalizedWeights: Map[CubeId, NormalizedWeight],
      desiredCubeSize: Int): Map[CubeId, CubeId] = {
    if (cubeNormalizedWeights.isEmpty ||
      (cubeNormalizedWeights.size == 1 && cubeNormalizedWeights.keys.head.isRoot)) {
      Map.empty[CubeId, CubeId]
    } else {
      val compressionMap = mutable.Map.newBuilder[CubeId, CubeId]
      compressionMap.sizeHint(cubeNormalizedWeights.size)

      cubeNormalizedWeights
        .filterNot(_._1.children.exists(cubeNormalizedWeights.contains))
        .mapValues(desiredCubeSize / _)
        .groupBy(_._1.parent.get)
        .foreach { case (parent, leafSizes) =>
          val leafSizeIter = leafSizes.toSeq.sortBy(_._2).toIterator
          var continue = true
          var accSize = 0L
          while (continue && leafSizeIter.hasNext) {
            val (leaf, leafSize) = leafSizeIter.next()
            if (accSize + leafSize <= desiredCubeSize) {
              accSize += leafSize.toInt
              compressionMap += leaf -> parent
            } else {
              continue = false
            }
          }
        }

      compressionMap.result().toMap
    }
  }

}
