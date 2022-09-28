package io.qbeast.core.model

import scala.collection.mutable

/**
 * Methods for normalizing weights.
 */
object CubeNormalizedWeights {

  /**
   * This method return the merge between the previous state of the index status
   * and the newly computed NormalizedCubeWeights.
   * @param previousStateNormalizedWeights the previous state of the NormalizedCubeWeights
   * @param deltaNormalizedCubeWeights the newly computed NormalizedCubeWeights
   * @return
   */
  def mergeNormalizedWeights(
      previousStateNormalizedWeights: Map[CubeId, NormalizedWeight],
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight]): Map[CubeId, Weight] = {
    {
      if (previousStateNormalizedWeights.isEmpty) {
        deltaNormalizedCubeWeights
      } else if (deltaNormalizedCubeWeights.isEmpty) {
        previousStateNormalizedWeights
      } else {
        val builder = Map.newBuilder[CubeId, NormalizedWeight]
        for ((cubeId, weight) <- previousStateNormalizedWeights) {
          val mergedWeight = deltaNormalizedCubeWeights.get(cubeId) match {
            case Some(estimatedWeight) => NormalizedWeight.merge(weight, estimatedWeight)
            case None => weight
          }
          builder += (cubeId -> mergedWeight)
        }
        for ((cubeId, estimatedWeight) <- deltaNormalizedCubeWeights) {
          if (!previousStateNormalizedWeights.contains(cubeId)) {
            builder += (cubeId -> estimatedWeight)
          }
        }
        builder.result()
      }
    }.mapValues(NormalizedWeight.toWeight)
      .map(identity)
    // We need this because mapValues is not serializable https://github.com/scala/bug/issues/7005

  }

  /**
   * Merge existing cube weights with the newly computed cube weights, and perform tree compression
   * through an up rolling operation. The latter outputs a descendant to ancestor CubeID mapping.
   * @param previousStateNormalizedWeights existing cube weights
   * @param deltaNormalizedCubeWeights cube weights to add
   * @param desiredCubeSize desired size for otree cubes
   * @return
   */
  def treeCompression(
      previousStateNormalizedWeights: Map[CubeId, NormalizedWeight],
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
      cubeStates: Map[CubeId, String],
      desiredCubeSize: Int): Map[CubeId, CubeId] = {
    val cubeSizes = mutable.Map.newBuilder[CubeId, Long]
    val cubeRollUpMap = mutable.Map.newBuilder[CubeId, CubeId]
    var maxDepth = 0

    for ((cubeId, normalizedWeight) <- deltaNormalizedCubeWeights) {
      // number of elements in a given cube is proportional to the change of the normalized weight
      val size = previousStateNormalizedWeights.get(cubeId) match {
        case Some(previousWeight) => (previousWeight - normalizedWeight) * desiredCubeSize
        case None => normalizedWeight * desiredCubeSize
      }
      cubeSizes += (cubeId -> size.toLong)
      cubeRollUpMap += (cubeId -> cubeId)
      maxDepth = maxDepth.max(cubeId.depth)
    }

    val rollUpMap = if (maxDepth > 0) {
      accumulativeRollUp(
        cubeRollUpMap.result(),
        cubeSizes.result(),
        cubeStates,
        maxDepth,
        desiredCubeSize)
    } else {
      Map.empty[CubeId, CubeId]
    }

    rollUpMap
  }

  /**
   * Sibling payloads are gathered together and placed in their parent cube
   * in a recursive and accumulative fashion until a size limit is reached
   * @param cubeRollUpMap mutable map that points each cube to themselves
   * @param cubeSizes the payload sizes of all cubes
   * @param maxDepth tree height
   * @param maxRollingSize the maximum payload size of a cube for it to be
   *                       continually rolling upwards
   * @return Descendant to ancestor cube mapping
   */
  def accumulativeRollUp(
      cubeRollUpMap: mutable.Map[CubeId, CubeId],
      cubeSizes: mutable.Map[CubeId, Long],
      cubeStates: Map[CubeId, String],
      maxDepth: Int,
      maxRollingSize: Int): Map[CubeId, CubeId] = {
    val levelCubeSizes = cubeSizes.groupBy(_._1.depth)

    (maxDepth to 1 by -1)
      .filter(levelCubeSizes.contains)
      .foreach { depth =>
        // Cube sizes from level depth
        levelCubeSizes(depth)
          // Filter out those with a size >= threshold
          .filter { case (_, size) => size < maxRollingSize * 0.8 }
          // Group cubes by parent cube to find sibling cubes
          .groupBy { case (cube, _) => cube.parent.get }
          // Make sure the parent cube exists
          .filter { case (parent, _) => cubeSizes.contains(parent) }
          // Process cubes from the current level
          .foreach { case (parent, siblingCubeSizes) =>
            // Add children sizes to parent size
            val newSize = cubeSizes(parent) + siblingCubeSizes.values.sum
            if (newSize <= maxRollingSize) {
              // update parent size
              cubeSizes(parent) = newSize
              // update mapping graph edge for sibling cubes
              siblingCubeSizes.keys.foreach(c => cubeRollUpMap(c) = parent)
            }
          }
      }

    cubeRollUpMap.keys
      .map(cube => {
        var targetCube: CubeId = cubeRollUpMap(cube)
        while (cubeRollUpMap(targetCube) != targetCube) {
          targetCube = cubeRollUpMap(targetCube)
        }
        (cube, targetCube)
      })
      .toMap
  }

}
