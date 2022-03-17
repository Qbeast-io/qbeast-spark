/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.keeper

import io.qbeast.SerializedCubeID
import io.qbeast.core.model.QTableID

import java.util.concurrent.atomic.AtomicInteger

/**
 * Implementation of Keeper which saves announced set in memory. This implementation is used
 * when no other implementation can be obtained from ServiceLoader.
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
