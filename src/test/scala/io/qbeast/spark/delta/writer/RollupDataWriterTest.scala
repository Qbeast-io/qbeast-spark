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
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.transform.EmptyTransformer
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.delta.actions.AddFile
import org.scalatest.PrivateMethodTester

import scala.reflect.io.Path

class RollupDataWriterTest extends QbeastIntegrationTestSpec with PrivateMethodTester {

  "RollupDataWriter" should "write the data correctly" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      import spark.implicits._
      val df = spark
        .range(size)
        .map(i =>
          Client4(
            i * i,
            s"student-$i",
            Some(i.toInt),
            Some(i * 1000 + 123),
            Some(i * 2567.3432143)))
        .toDF()

      val tableID = QTableID(tmpDir)
      val options =
        QbeastOptions(Map("columnsToIndex" -> "age,val2", "cubeSize" -> cubeSize.toString))
      val revision =
        SparkRevisionFactory.createNewRevision(tableID, df.schema, options)
      val indexStatus = IndexStatus(revision)
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)

      val fileActions =
        RollupDataWriter.write(tableID, df.schema, options, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
        fa.dataChange shouldBe true
      }
    }

  it should "deactivate rollup when using a small rollupSize" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      import spark.implicits._
      val df = spark
        .range(size)
        .map(i =>
          Client4(
            i * i,
            s"student-$i",
            Some(i.toInt),
            Some(i * 1000 + 123),
            Some(i * 2567.3432143)))
        .toDF()

      val tableID = QTableID(tmpDir)
      val options =
        QbeastOptions(
          Map(
            "columnsToIndex" -> "age,val2",
            "cubeSize" -> cubeSize.toString,
            "rollupSize" -> "0"))
      val revision =
        SparkRevisionFactory.createNewRevision(tableID, df.schema, options)
      val indexStatus = IndexStatus(revision)
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)
      val fileActions =
        RollupDataWriter.write(tableID, df.schema, options, qbeastData, tableChanges)

      val indexFiles = fileActions
        .collect { case a: AddFile => a }
        .map(IndexFiles.fromAddFile(2))

      indexFiles.foreach(f => f.blocks.size shouldBe 1)
  }

  it should "group all blocks into one file when rollupSize is >= df.count" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      import spark.implicits._
      val df = spark
        .range(size)
        .map(i =>
          Client4(
            i * i,
            s"student-$i",
            Some(i.toInt),
            Some(i * 1000 + 123),
            Some(i * 2567.3432143)))
        .toDF()

      val tableID = QTableID(tmpDir)
      val options =
        QbeastOptions(
          Map(
            "columnsToIndex" -> "age,val2",
            "cubeSize" -> cubeSize.toString,
            "rollupSize" -> size.toString))
      val revision =
        SparkRevisionFactory.createNewRevision(tableID, df.schema, options)
      val indexStatus = IndexStatus(revision)
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)
      val fileActions =
        RollupDataWriter.write(tableID, df.schema, options, qbeastData, tableChanges)

      val addFiles = fileActions.collect { case a: AddFile => a }
      addFiles.size shouldBe 1
  }

  it should "compute rollup correctly when optimizing" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val computeRollup = PrivateMethod[Map[CubeId, CubeId]]('computeRollup)
      val dcs = 20
      val revision =
        Revision(1L, 0, QTableID(tmpDir), dcs, Vector(EmptyTransformer("col_1")), Vector.empty)

      val root = revision.createCubeIdRoot()
      val c1 = root.children.next()
      val c2 = c1.nextSibling.get
      val extendedData = Seq(
        (1 to 20).map(_ => ("root", root.bytes)),
        (1 to 1).map(_ => ("c1", c1.bytes)),
        (1 to 20).map(_ => ("c2", c2.bytes))).flatten.toDF("id", QbeastColumns.cubeColumnName)

      val rollup = RollupDataWriter invokePrivate computeRollup(revision, extendedData, dcs)
      rollup shouldBe Map(root -> root, c1 -> root, c2 -> c2)
    }

}
