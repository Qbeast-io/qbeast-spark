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
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight])
      : Map[CubeId, NormalizedWeight] = {
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
    } // .mapValues(NormalizedWeight.toWeight)
//      .map(identity)
    // We need this because mapValues is not serializable https://github.com/scala/bug/issues/7005

  }

  /**
   * Perform tree compression through an up rolling operation to create a
   * descendant to ancestor CubeID mapping.
   * @param previousStateNormalizedWeights existing cube weights
   * @param mergedNormalizedCubeWeights cube weights to add
   * @param desiredCubeSize desired size for otree cubes
   * @return
   */
  def treeCompression(
      previousStateNormalizedWeights: Map[CubeId, NormalizedWeight],
      mergedNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
      announcedOrReplicatedSet: Set[CubeId],
      desiredCubeSize: Int): Map[CubeId, CubeId] = {
    val cubeSizes = mutable.Map.newBuilder[CubeId, Long]
    val cubeRollUpMap = mutable.Map.newBuilder[CubeId, CubeId]
    var maxDepth = 0

    for ((cubeId, normalizedWeight) <- mergedNormalizedCubeWeights) {
      // The number of elements in a given cube is proportional to the
      // change of its normalized weight
      val size = previousStateNormalizedWeights.get(cubeId) match {
        case Some(previousWeight) =>
          ((previousWeight - normalizedWeight) * desiredCubeSize).max(desiredCubeSize / 2)
        case _ if normalizedWeight < 1.0 => desiredCubeSize
        case _ => desiredCubeSize / normalizedWeight
      }
      cubeSizes += (cubeId -> size.toLong)
      cubeRollUpMap += (cubeId -> cubeId)
      maxDepth = maxDepth.max(cubeId.depth)
    }

    // scalastyle:off println
    println(s"Estimated initial cubeSizes:")
    cubeSizes.result().toSeq.sortBy(_._1).foreach { case (cube, size) =>
      val weight = mergedNormalizedCubeWeights(cube)
      println(s"cube: $cube, size: $size, NormalizedWeight: $weight")
    }

    val rollUpMap = if (maxDepth > 0) {
      accumulativeRollUp(
        cubeRollUpMap.result(),
        cubeSizes.result(),
        announcedOrReplicatedSet,
        maxDepth,
        (desiredCubeSize * 1.2).toInt)
    } else {
      Map.empty[CubeId, CubeId]
    }

    rollUpMap
  }

  /**
   * Sibling payloads are gathered together and placed in their parent cube in a recursive
   * and accumulative fashion until a size limit is reached, provided that the node itself
   * nor its parent node are REPLICATED or ANNOUNCED.
   * @param cubeRollUpMap mutable map that points each cube to themselves
   * @param cubeSizes the payload sizes of all cubes
   * @param announcedOrReplicatedSet the set of ANNOUNCED or REPLICATED cubes
   * @param maxDepth tree height
   * @param maxRollingSize the maximum payload size of a cube for it to be
   *                       continually rolling upwards
   * @return Descendant to ancestor cube mapping
   */
  def accumulativeRollUp(
      cubeRollUpMap: mutable.Map[CubeId, CubeId],
      cubeSizes: mutable.Map[CubeId, Long],
      announcedOrReplicatedSet: Set[CubeId],
      maxDepth: Int,
      maxRollingSize: Int): Map[CubeId, CubeId] = {
    val levelCubeSizes = cubeSizes.groupBy(_._1.depth)

    // Iterate from maxDepth to level 1, avoiding the root
    (maxDepth to 1 by -1)
      .filter(levelCubeSizes.contains)
      .foreach { depth =>
        // Cube sizes from the current level
        levelCubeSizes(depth)
          // We operate on cubes that are neither ANNOUNCED nor REPLICATED, for these have
          // replications from their ancestors, performing roll-up will corrupt OTree integrity
          .filter { case (cube, _) => !announcedOrReplicatedSet.contains(cube) }
          // Group cubes by their parent in order to find sibling cubes
          .groupBy { case (cube, _) => cube.parent.get }
          // Make sure parent cube existing in the map and it's not an ANNOUNCED nor REPLICATED cube
          .filter { case (parent, _) =>
            cubeSizes.contains(parent) && !announcedOrReplicatedSet.contains(parent)
          }
          // Process sibling cubes from the current level
          .foreach { case (parent, siblingCubeSizes) =>
            // Take the smallest cube each time and add them to the parent until either
            // all cubes are rolled-up or the size has grown too large
            val sortedCubeIter = siblingCubeSizes.toSeq.sortBy(_._2).toIterator
            var newSize = 0L
            var continue = true
            while (continue && sortedCubeIter.hasNext) {
              val (cube, size) = sortedCubeIter.next()
              continue = newSize + size < maxRollingSize
              if (continue) {
                // Increment size
                newSize += size
                // Map child to parent
                cubeRollUpMap(cube) = parent
              }
            }
            // Update parent size
            cubeSizes(parent) = newSize + cubeSizes(parent)
          }
      }

    // Convert A -> B -> C -> D to A -> D
    val rollUpMap = cubeRollUpMap.keys
      .map(cube => {
        var targetCube: CubeId = cubeRollUpMap(cube)
        while (cubeRollUpMap(targetCube) != targetCube) {
          targetCube = cubeRollUpMap(targetCube)
        }
        (cube, targetCube)
      })
      .toMap

    // scalastyle:off println
    println(s"Compressed cubeSizes:")
    cubeSizes
      .result()
      .keys
      .foreach(cube => {
        val mappedCube = rollUpMap(cube)
        println(s"cube: $cube, mappedCube: $mappedCube, size: ${cubeSizes(mappedCube)}")
      })

    rollUpMap
  }

}
