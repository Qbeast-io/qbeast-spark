/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.keeper

import io.qbeast.SerializedCubeID
import io.qbeast.core.model.QTableID

import java.util.ServiceLoader

/**
 * Qbeast keeper.
 */
trait Keeper {

  /**
   * Begins a write for given index domain revision.
   *
   * @param tableID the table identifier
   * @param revision the domain revision
   * @return the write operation
   */
  def beginWrite(tableID: QTableID, revision: Long): Write

  /**
   * Runs an action as part of write operation for the specified index revision.
   *
   * @param tableID the table identifier
   * @param revision the index domain revision
   * @tparam T the result type
   * @return the result
   */
  def withWrite[T](tableID: QTableID, revision: Long)(action: Write => T): T = {
    val write = beginWrite(tableID, revision)
    try {
      action(write)
    } finally {
      write.end()
    }
  }

  /**
   * Announces cubes for given index domain revision
   * @param tableID the table identifier
   * @param revision the domain revision
   * @param cubes the announced cube identifiers
   */
  def announce(tableID: QTableID, revision: Long, cubes: Seq[SerializedCubeID]): Unit

  /**
   * Begins an optimization for given index domain revision.
   * @param tableID the table identifier
   * @param revision the domain revision
   * @param cubeLimit the maximum (exclusive) number of cubes to be optimized
   * @return the optimization operation
   */
  def beginOptimization(
      tableID: QTableID,
      revision: Long,
      cubeLimit: Integer = Integer.MAX_VALUE): Optimization

  /**
   * Stops the keeper and releases the allocated resources.
   */
  def stop(): Unit
}

/**
 * Optimization operation.
 */
trait Optimization {

  /**
   * The identifier.
   */
  val id: String

  /**
   * The cubes to optimize.
   */
  val cubesToOptimize: Set[SerializedCubeID]

  /**
   * Ends the optimization.
   *
   * @param replicatedCubes the replicated cube identifiers
   */
  def end(replicatedCubes: Set[SerializedCubeID]): Unit
}

/**
 * Write operation.
 */
trait Write {

  /**
   * The identifier.
   */
  val id: String

  /**
   * The announced cube identifiers
   */
  val announcedCubes: Set[SerializedCubeID]

  /**
   * Ends the write.
   */
  def end(): Unit
}

/**
 * Keeper companion object.
 */
object Keeper {

  /**
   * Creates a Keeper instance for a given configuration.
   *
   * @param config the configuration
   * @return a Keeper instance
   */
  def apply(config: Map[String, String]): Keeper = {
    val loader = ServiceLoader.load(classOf[KeeperFactory])
    val iterator = loader.iterator()
    if (iterator.hasNext) {
      iterator.next().createKeeper(config)
    } else {
      LocalKeeper
    }
  }

}

/**
 * Factory for creating Keeper instances. This interface
 * should be implemented and deployed by external libraries as follows:
 * <ul>
 *   <li>Implement this interface in a class which has public no-argument constructor</li>
 *   <li>Register the implementation according to ServiceLoader specification</li>
 *   <li>Add the jar with the implementation to the application classpath</li>
 * </ul>
 */
trait KeeperFactory {

  /**
   * Creates a new keeper for a given configuration.
   *
   * @param config the configuration
   * @return a new keeper
   */
  def createKeeper(config: Map[String, String]): Keeper
}
