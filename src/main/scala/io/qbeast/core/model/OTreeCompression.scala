/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import scala.collection.mutable

object OTreeCompression {

  def estimateCubeSizes(
      mergedNormalizedWeights: Map[CubeId, NormalizedWeight],
      existingNormalizedWeights: Map[CubeId, NormalizedWeight],
      desiredCubeSize: Int): mutable.Map[CubeId, Long] = {
    val cubeSizes = mutable.Map.newBuilder[CubeId, Long]
    cubeSizes.sizeHint(mergedNormalizedWeights.size)

    for ((cube, normalizedWeight) <- mergedNormalizedWeights) {
      val size = existingNormalizedWeights.get(cube) match {
        case Some(existingWeight) =>
          (existingWeight - normalizedWeight).max(0.5) * desiredCubeSize
        case _ if normalizedWeight < 1.0 => desiredCubeSize
        case _ => desiredCubeSize / normalizedWeight
      }
      cubeSizes += cube -> size.toLong
    }

    cubeSizes.result()
  }

  def leafRollUp(
      cubeSizes: mutable.Map[CubeId, Long],
      announcedOrReplicatedSet: Set[CubeId],
      maxRollingSize: Int): Map[CubeId, CubeId] = {
    if (cubeSizes.size == 1 && cubeSizes.keys.head.isRoot) {
      Map.empty[CubeId, CubeId]
    } else {
      val cubeRollUpMap = mutable.Map.newBuilder[CubeId, CubeId]
      cubeSizes
        .filterNot(_._1.children.exists(cubeSizes.contains))
        .groupBy(_._1.parent.get)
        .filter { case (parent, _) =>
          cubeSizes.contains(parent) && !announcedOrReplicatedSet.contains(parent)
        }
        .foreach { case (parent, leafSizes) =>
          val sortedLeafCubeIter =
            leafSizes
              .filterNot(ccs => announcedOrReplicatedSet.contains(ccs._1))
              .toSeq
              .sortBy(_._1)
              .toIterator
          var continue = true
          var accSize = 0L
          while (continue && sortedLeafCubeIter.hasNext) {
            val (leaf, leafSize) = sortedLeafCubeIter.next()
            if (accSize + leafSize <= maxRollingSize) {
              accSize += leafSize
              cubeRollUpMap += leaf -> parent
            } else {
              continue = false
            }
          }
          cubeSizes(parent) += accSize
        }

      cubeRollUpMap.result().toMap
    }
  }

  def shortenGraphEdges(rollUpMap: mutable.Map[CubeId, CubeId]): mutable.Map[CubeId, CubeId] = {
    // Convert A -> B -> C -> D to A -> D
    rollUpMap.keys
      .foreach(cube => {
        var targetCube: CubeId = rollUpMap(cube)
        while (rollUpMap(targetCube) != targetCube) {
          targetCube = rollUpMap(targetCube)
        }
        rollUpMap(cube) = targetCube
      })

    rollUpMap
  }

  /**
   * Sibling payloads are gathered together and placed in their parent cube in a recursive
   * and accumulative fashion until a size limit is reached, provided that the node itself
   * nor its parent node are REPLICATED or ANNOUNCED.
   * @param cubeSizes the payload sizes of all cubes
   * @param announcedOrReplicatedSet the set of ANNOUNCED or REPLICATED cubes
   * @param maxRollingSize the maximum payload size of a cube for it to be
   *                       continually rolling upwards
   * @return Descendant to ancestor cube mapping
   */
  def accumulativeRollUp(
      cubeSizes: mutable.Map[CubeId, Long],
      announcedOrReplicatedSet: Set[CubeId],
      maxRollingSize: Int): Map[CubeId, CubeId] = {
    val cubeRollUpMap = mutable.Map.newBuilder[CubeId, CubeId]
    cubeRollUpMap.sizeHint(cubeSizes.size)

    val cubesPerLevel = cubeSizes.keys.groupBy(_.depth)
    val maxDepth = cubesPerLevel.keys.max
    // Iterate from maxDepth to level 1, avoiding the root
    (maxDepth to 1 by -1)
      .filter(cubesPerLevel.contains)
      .foreach { depth =>
        // Cube sizes from the current level
        cubesPerLevel(depth)
          // We operate on cubes that are neither ANNOUNCED nor REPLICATED, for these have
          // replications from their ancestors, performing roll-up will corrupt OTree integrity
          .filterNot(announcedOrReplicatedSet.contains)
          // Group cubes by their parent in order to find sibling cubes
          .groupBy(_.parent.get)
          // Make sure parent cube existing in the map and it's not an ANNOUNCED nor REPLICATED cube
          .filter { case (parent, _) =>
            cubeSizes.contains(parent) && !announcedOrReplicatedSet.contains(parent)
          }
          // Process sibling cubes from the current level
          .foreach { case (parent, siblingCubes) =>
            // Take the smallest cube each time and add them to the parent until either
            // all cubes are rolled-up or the size has grown too large
            val sortedCubeIter =
              siblingCubes.map(c => (c, cubeSizes(c))).toSeq.sortBy(_._2).toIterator
            var accSize = 0L
            var continue = true
            while (continue && sortedCubeIter.hasNext) {
              val (child, size) = sortedCubeIter.next()
              if (accSize + size <= maxRollingSize) {
                // Increment size
                accSize += size
                // Map child to parent
                cubeRollUpMap += child -> parent
              } else {
                continue = false
              }
            }
            // Update parent size
            cubeSizes(parent) += accSize
          }
      }

    shortenGraphEdges(cubeRollUpMap.result()).toMap
  }

//  def updateMergedNormalizedWeights(
//      rollUpMap: Map[CubeId, CubeId],
//      cubeSizes: Map[CubeId, Long],
//      mergedNormalizedWeights: Map[CubeId, NormalizedWeight],
//      desiredCubeSize: Int): Map[CubeId, NormalizedWeight] = {
//    mergedNormalizedWeights ++ mergedNormalizedWeights
//      .filterNot(_._1.children.exists(mergedNormalizedWeights.contains))
//      .map { case (leaf, _) => leaf -> 1.0 }
//  }

  /**
   * Perform tree compression through an up rolling operation to create a
   * descendant to ancestor CubeID mapping.
   * @param mergedNormalizedCubeWeights cube weights to add
   * @param previousStateNormalizedWeights existing cube weights
   * @param desiredCubeSize desired size for otree cubes
   * @return
   */
  def treeCompression(
      mergedNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
      previousStateNormalizedWeights: Map[CubeId, NormalizedWeight],
      announcedOrReplicatedSet: Set[CubeId],
      desiredCubeSize: Int,
      compressionStrategy: (mutable.Map[CubeId, Long], Set[CubeId], Int) => Map[CubeId, CubeId])
      : Map[CubeId, CubeId] = {
    val cubeSizes = estimateCubeSizes(
      mergedNormalizedCubeWeights,
      previousStateNormalizedWeights,
      desiredCubeSize)

    if (cubeSizes.nonEmpty) {
      val rollUpMap = compressionStrategy(cubeSizes, announcedOrReplicatedSet, desiredCubeSize)
      rollUpMap
    } else {
      Map.empty[CubeId, CubeId]
    }
  }

}
