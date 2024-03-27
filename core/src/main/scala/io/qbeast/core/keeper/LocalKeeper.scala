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
package io.qbeast.core.keeper

import io.qbeast.core.model.QTableID
import io.qbeast.SerializedCubeID

import java.util.concurrent.atomic.AtomicInteger

/**
 * Implementation of Keeper which saves announced set in memory. This implementation is used when
 * no other implementation can be obtained from ServiceLoader.
 */
object LocalKeeper extends Keeper {
  private val generator = new AtomicInteger()

  private val announcedMap =
    scala.collection.mutable.Map.empty[(QTableID, Long), Set[SerializedCubeID]]

  override def beginWrite(tableID: QTableID, revision: Long): Write = new LocalWrite(
    generator.getAndIncrement().toString,
    announcedMap.getOrElse((tableID, revision), Set.empty[SerializedCubeID]))

  override def announce(tableID: QTableID, revision: Long, cubes: Seq[SerializedCubeID]): Unit = {
    val announcedCubes = announcedMap.getOrElse((tableID, revision), Set.empty[SerializedCubeID])
    announcedMap.update((tableID, revision), announcedCubes.union(cubes.toSet))
  }

  override def beginOptimization(
      tableID: QTableID,
      revision: Long,
      cubeLimit: Integer): Optimization =
    new LocalOptimization(
      generator.getAndIncrement().toString,
      announcedMap.getOrElse((tableID, revision), Set.empty[SerializedCubeID]))

  override def stop(): Unit = {}
}

private class LocalWrite(val id: String, override val announcedCubes: Set[SerializedCubeID])
    extends Write {

  override def end(): Unit = {}
}

private class LocalOptimization(
    val id: String,
    override val cubesToOptimize: Set[SerializedCubeID])
    extends Optimization {

  override def end(replicatedCubes: Set[SerializedCubeID]): Unit = {}
}
