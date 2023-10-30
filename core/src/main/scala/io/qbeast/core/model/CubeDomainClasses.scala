package io.qbeast.core.model

import io.qbeast.core.model.Weight.MaxValue

private[model] object WeightAndCountFactory {

  def apply(
      desiredCubeSize: Int,
      groupCubeSize: Int,
      cubeStatuses: Map[CubeId, CubeStatus]): WeightAndCountFactory = {
    val isLeaf = (cubeId: CubeId) => !cubeId.children.exists(cubeStatuses.contains)
    val multiplier = groupCubeSize / desiredCubeSize.toDouble

    val cubeNormalizedWeights = cubeStatuses.map {
      case (cubeId, status) if isLeaf(cubeId) =>
        val cubeSize = status.files.map(_.elementCount).sum
        val groupSize = (cubeSize * multiplier).toLong.max(1L)
        val nw = NormalizedWeight(groupCubeSize, groupSize).max(1.0)
        cubeId -> nw
      case (cubeId, status) => cubeId -> status.normalizedWeight
    }

    new WeightAndCountFactory(cubeNormalizedWeights, groupCubeSize)
  }

}

/**
 * Factory for WeightAndCount. The creation of which depends on whether the associated
 * CubeId is present in the existing index, if it's a inner or leaf cube.
 *
 * @param existingNormalizedWeights Cube Weight of the existing index
 * @param groupCubeSize partition-level cube size for indexing
 */
private[model] class WeightAndCountFactory(
    existingNormalizedWeights: Map[CubeId, NormalizedWeight],
    groupCubeSize: Int) {

  def createWeightAndCount(cubeId: CubeId): WeightAndCount =
    existingNormalizedWeights.get(cubeId) match {
      case Some(nw: NormalizedWeight) if nw < 1.0 =>
        new InnerCubeWeightAndCount(Weight(nw))
      case Some(nw: NormalizedWeight) =>
        val existingLeafSize = ((groupCubeSize - 1) / nw).toInt
        new LeafCubeWeightAndCount(existingLeafSize)
      case None => new WeightAndCount(MaxValue, 0)
    }

}

/**
 * Weight and count for a CubeId.
 * @param weight Weight of the associated CubeId
 * @param count Element count of the associated CubeId
 */
private[model] class WeightAndCount(var weight: Weight, var count: Int) {

  /**
   * Determines whether an instance, with its instanceWeight, should be included
   * in the associated cube
   * @param instanceWeight Weight of a given instance
   * @param limit total capacity of the cube i.e. groupCubeSize
   */
  def shouldInclude(instanceWeight: Weight, limit: Int): Boolean =
    count < limit

  /**
   * Update metadata to include the instance
   * @param instanceWeight Weight of a given instance
   */
  def update(instanceWeight: Weight): Unit = {
    count += 1
    weight = instanceWeight
  }

  /**
   * Convert to WeightAndTreeSize, with cube size i.e. count as the initial tree size value.
   * @param limit the groupCubeSize used
   */
  def toWeightAndTreeSize(limit: Int): WeightAndTreeSize = {
    val nw = toNormalizedWeight(limit)
    new WeightAndTreeSize(nw, cubeSize)
  }

  /**
   * @return the number of added instances to the associated cube
   */
  def cubeSize: Int = count

  /**
   * Compute NormalizedWeight according to the state of the cube
   * @param limit groupCubeSize
   */
  def toNormalizedWeight(limit: Int): NormalizedWeight =
    if (count == limit) NormalizedWeight(weight)
    else NormalizedWeight(limit, cubeSize)

}

/**
 * WeightAndCount for an existing leaf cube.
 * It can accept up to (groupCubeSize - start) records.
 */
private[model] class LeafCubeWeightAndCount(start: Int) extends WeightAndCount(MaxValue, start) {

  override def cubeSize: Int = count - start

}

/**
 * WeightAndCount for an existing inner cube.
 * It can accept up to groupCubeSize records, with the additional constraint
 * of instanceWeight < existingWeight.
 * @param existingWeight Weight for the associated existing CubeId
 */
private[model] class InnerCubeWeightAndCount(existingWeight: Weight)
    extends WeightAndCount(existingWeight, 0) {

  override def shouldInclude(instanceWeight: Weight, limit: Int): Boolean = {
    instanceWeight < existingWeight && count < limit
  }

  override def toNormalizedWeight(limit: Int): NormalizedWeight = {
    // An existing inner cube should always remain as such
    NormalizedWeight(weight)
  }

}

/**
 * Point, weight and parent cube identifier if available.
 *
 * @param point  the point
 * @param weight the weight
 * @param parent the parent
 */
private[model] case class PointWeightAndParent(
    point: Point,
    weight: Weight,
    parent: Option[CubeId])

/**
 * NormalizedWeight and tree size of a given cube, with tree size defined as the
 * sum of its own payload and all that of its descendents. The tree size is
 * initialized with cube size and updated to the tree size during computation
 * before being used.
 * @param weight NormalizedWeight
 * @param treeSize Cube tree size
 */
private[model] class WeightAndTreeSize(val weight: NormalizedWeight, var treeSize: Double)
