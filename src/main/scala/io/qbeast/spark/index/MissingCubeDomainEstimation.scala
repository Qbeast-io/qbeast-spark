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

  /**
   * For the closest existing ancestor, compute its payload fraction
   * and domain contribution to the missing cube.
   */
  private[index] def initialFractionAndDomain(
      ancestor: CubeId,
      localTree: LocalTree): (Double, Double) = {
    // When CA is leaf, consider its payload to be entirely of our target domain.
    // Otherwise, compute fraction via subtree sizes, supposing the missing cube
    // has 1 element.
    val subtree = ancestor.children.filter(localTree.contains)
    val f = if (subtree.nonEmpty) {
      val subtreeSize = subtree.map(localTree(_).treeSize).sum + 1d
      1d / subtreeSize
    } else 1d

    (f, 0d)
  }

  /**
   * Given a CubeId C and its LocalTree, compute the size of its parent payload as well as its
   * fraction that fall within the subspace of C. C and C.parent are assumed to be present in
   * the given LocalTree.
   */
  private[index] def parentFractionAndPayload(
      ancestor: CubeId,
      localTree: LocalTree): (Double, Double) = {
    // The fraction f from A's payload that belongs to C's branch is computed as:
    // f = TreeSize(C) / (TreeSize(B) + TreeSize(C), TreeSize(D))
    // LocalTree:
    //                    A
    //                  / | \
    //                 B  C  D
    val ancestorTreeSize = localTree(ancestor).treeSize

    val parent = ancestor.parent.get
    val parentTreeSize = localTree(parent).treeSize
    val siblingTreeSize =
      parent.children.filter(localTree.contains).map(localTree(_).treeSize).sum

    val f = ancestorTreeSize / siblingTreeSize
    val payload = parentTreeSize - siblingTreeSize

    (f, payload)
  }

  /**
   *  For each existing ancestor of the missing cube, compute the fraction of
   *  their payload that belongs to our target domain. The payload distribution of a given cube
   *  is assumed to be the same as that of its child cube tree sizes.
   */
  private[index] def domainThroughPayloadFractions(
      missingCube: CubeId,
      localTree: LocalTree): Double = {
    findClosestAncestorBottomUp(localTree, missingCube) match {
      case None => 0d
      case Some(cube) =>
        var ancestor = cube
        var (payloadFraction, localDomain) = initialFractionAndDomain(ancestor, localTree)
        // Stop the computation when the ancestor is root or,
        // its parent is missing from the LocalTree, which happens during optimization.
        while (!ancestor.isRoot && localTree.contains(ancestor.parent.get)) {
          val (fraction, payload) = parentFractionAndPayload(ancestor, localTree)
          // Update fraction using accumulative product, and update the domain.
          payloadFraction *= fraction
          localDomain += payload * payloadFraction

          ancestor = ancestor.parent.get
        }

        localDomain
    }
  }

}
