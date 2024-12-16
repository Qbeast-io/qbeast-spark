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
package io.qbeast.spark.writer

import io.qbeast.core.model.CubeId

import scala.collection.mutable

/**
 * Rollup represents a single rollup operation which begins with populating the instance with data
 * to be rolled up and completes with computation of the rollup result. Instances of this class
 * should not be reused.
 *
 * @param limit
 *   the limit for the sum of the cube sizes rolled up to the same cube
 */
private[writer] class Rollup(limit: Double) {

  private val groups = mutable.Map.empty[CubeId, Group]

  /**
   * Populate this instance with given cube identifier and size. If this method is called several
   * times with the same cube identifier then the provided sized are summed up.
   *
   * @param cubeId
   *   the cube identifier
   * @param size
   *   the size associated with the cube
   */
  def populate(cubeId: CubeId, size: Long): Rollup = {
    val group = groups.getOrElseUpdate(cubeId, Group.empty)
    group.add(cubeId, size)
    this
  }

  /**
   * Computes the rollup result and returns the map where the keys are the original cube
   * identifiers and the values are the cube identifiers to which the original data is rolled up.
   *
   * @return
   *   the rollup result
   */
  def compute(): Map[CubeId, CubeId] = {
    val queue = new mutable.PriorityQueue()(CubeIdRollupOrdering)
    groups.keys.foreach(queue.enqueue(_))
    while (queue.nonEmpty) {
      val cubeId = queue.dequeue()
      val group = groups(cubeId)
      if (group.size < limit && !cubeId.isRoot) {
        val nextInLine = queue.headOption match {
          case Some(cube) if areSiblings(cube, cubeId) => cube
          case _ => cubeId.parent.get
        }
        if (groups.contains(nextInLine)) {
          groups(nextInLine).add(group)
        } else {
          groups.put(nextInLine, group)
          queue.enqueue(nextInLine)
        }
        groups.remove(cubeId)
      }
    }
    groups.flatMap { case (rollupCubeId, group) =>
      group.cubeIds.map((_, rollupCubeId))
    }.toMap
  }

  /**
   * Checks if the given cube identifiers are siblings. Two cube identifiers are siblings if they
   * have the same parent. It is assumed that the cube identifiers are different.
   */
  private def areSiblings(cube_a: CubeId, cube_b: CubeId): Boolean =
    cube_a.parent == cube_b.parent

  /*
   * Ordering for cube identifiers. The cube identifiers are ordered by their depth in ascending
   * order. If the depth is the same then the cube identifiers are ordered by in reverse order.
   * This ordering is used in the priority queue to process the cube identifiers in the correct
   * order, i.e., from the deepest to the shallowest, and from the leftmost to the rightmost:
   *    0                     root
   *    1           c0                   c1
   *    2     c00         c01       c10         c11
   * The priority queue will process the cube identifiers in the following order:
   * c00, c01, c10, c11, c0, c1, root.
   * c00 -> c01 -> c0, c10 -> c11 -> c1, c0 -> c1 -> root
   */
  private[writer] object CubeIdRollupOrdering extends Ordering[CubeId] {

    override def compare(x: CubeId, y: CubeId): Int = {
      val depthComparison = x.depth.compareTo(y.depth)
      if (depthComparison == 0) {
        y.compare(x)
      } else {
        depthComparison
      }
    }

  }

  private class Group(val cubeIds: mutable.Set[CubeId], var size: Long) {

    def add(cubeId: CubeId, size: Long): Unit = {
      cubeIds.add(cubeId)
      this.size += size
    }

    def add(other: Group): Unit = {
      other.cubeIds.foreach(cubeIds.add)
      size += other.size
    }

    override def toString: String = s"[Group: cubeIds: $cubeIds, size: $size]"

  }

  private object Group {
    def empty: Group = new Group(mutable.Set.empty, 0L)
  }

}
