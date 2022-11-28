/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, LocalTree}

object MissingCubeDomainEstimation {

  /**
   * Find the closest existing ancestor of a missing cube in the local tree,
   * starting from the immediate parent of the missing cube.
   */
  private[index] def findClosestAncestorBottomUp(
      localTree: LocalTree,
      missingCube: CubeId): Option[CubeId] = {
    // Find the first ancestor cube of the missingCube in a bottom-up fashion.
    var parentOption = missingCube.parent
    // Loop ends when either parentOption is None, or when the parent is present in the tree.
    while (parentOption.isDefined && !localTree.contains(parentOption.get)) {
      parentOption = parentOption.get.parent
    }
    parentOption
  }

}
