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
package io.qbeast.spark.internal.sources

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.InsertableRelation

import scala.util.Random

class QbeastBaseRelationTest extends QbeastIntegrationTestSpec {

  "QbeastBaseRelation" should "output a HadoopFsRelation with Insertable" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        1.to(10).map(i => Student(i, i.toString, Random.nextInt())).toDF("id", "name", "age")

      df.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      val indexedTable = QbeastContext.indexedTableFactory.getIndexedTable(QTableID(tmpDir))
      QbeastBaseRelation
        .forQbeastTable(indexedTable) shouldBe a[HadoopFsRelation with InsertableRelation]
    })

  it should "save new data" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df =
      1.to(10).map(i => Student(i, i.toString, Random.nextInt())).toDF("id", "name", "age")

    df.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

    val indexedTable = QbeastContext.indexedTableFactory.getIndexedTable(QTableID(tmpDir))
    val qbeastBaseRelation = QbeastBaseRelation.forQbeastTable(indexedTable)

    qbeastBaseRelation.asInstanceOf[InsertableRelation].insert(df, false)

    val indexed = spark.read.format("qbeast").load(tmpDir)
    indexed.count() shouldBe df.count() * 2 // we write two times the data

  })

  it should "save new data on empty table" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._

    // Create Staged Table with Spark SQL
    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    val indexedTable = QbeastContext.indexedTableFactory.getIndexedTable(QTableID(tmpDir))
    val qbeastBaseRelation =
      QbeastBaseRelation.forQbeastTableWithOptions(indexedTable, Map("columnsToIndex" -> "id"))

    // Insert new data
    val df =
      1.to(10).map(i => Student(i, i.toString, Random.nextInt())).toDF("id", "name", "age")

    qbeastBaseRelation.asInstanceOf[InsertableRelation].insert(df, false)

    val indexed = spark.read.format("qbeast").load(tmpDir)
    indexed.count() shouldBe df.count()

  })

}
