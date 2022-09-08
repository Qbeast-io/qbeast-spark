package io.qbeast.spark.keeper

import io.qbeast.core.keeper.{Keeper, Optimization, Write}
import io.qbeast.core.model.{CubeId, Point, QTableID}

import java.util.UUID
import scala.util.Random

/**
 * Randomized implementation of Keeper for testing.
 */
object RandomKeeper extends Keeper {

  private val containersOneDimension =
    CubeId.containers(Point(Random.nextDouble())).take(100).map(_.string).toSet

  override def beginWrite(tableID: QTableID, revision: Long): Write = {
    WriteImpl(UUID.randomUUID().toString, Set())
  }

  override def announce(tableID: QTableID, revision: Long, cubes: Seq[String]): Unit = {}

  override def beginOptimization(
      tableID: QTableID,
      revision: Long,
      cubeLimit: Integer): Optimization = {
    val cubesToOptimize =
      containersOneDimension.filter(_ => Random.nextInt() % 2 == 0)
    OptimizationImpl(UUID.randomUUID().toString, cubesToOptimize)
  }

  override def stop(): Unit = {}
}

private case class OptimizationImpl(id: String, cubesToOptimize: Set[String])
    extends Optimization {
  override def end(replicatedCubes: Set[String]): Unit = {}
}

private case class WriteImpl(id: String, announcedCubes: Set[String]) extends Write {
  override def end(): Unit = {}
}
