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

import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._

import scala.reflect.io.Path

class RollupDataWriterTest extends QbeastIntegrationTestSpec {

  "RollupDataWriter" should "write the data correctly" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      val df = spark.createDataFrame(spark.sparkContext.parallelize(0
        .to(size)
        .map(i =>
          Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143)))))

      val tableID = QTableID(tmpDir)
      val parameters: Map[String, String] =
        Map("columnsToIndex" -> "age,val2", "cubeSize" -> cubeSize.toString)
      val revision =
        SparkRevisionFactory.createNewRevision(tableID, df.schema, QbeastOptions(parameters))
      val indexStatus = IndexStatus(revision)
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)

      val fileActions = RollupDataWriter.write(tableID, df.schema, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
        fa.dataChange shouldBe true
      }
    }

}
