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
package io.qbeast.spark.keeper

import io.qbeast.core.keeper.Keeper
import io.qbeast.core.keeper.Optimization
import io.qbeast.core.keeper.Write
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.Point
import io.qbeast.core.model.QTableId

import java.util.UUID
import scala.util.Random

/**
 * Randomized implementation of Keeper for testing.
 */
object RandomKeeper extends Keeper {

  private val containersOneDimension =
    CubeId.containers(Point(Random.nextDouble())).take(100).map(_.string).toSet

  override def beginWrite(tableId: QTableId, revision: Long): Write = {
    WriteImpl(UUID.randomUUID().toString, Set())
  }

  override def announce(tableId: QTableId, revision: Long, cubes: Seq[String]): Unit = {}

  override def beginOptimization(
      tableId: QTableId,
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
