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

import io.qbeast.spark.internal.sources.catalog.CatalogTestSuite
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform

import scala.collection.JavaConverters._

class QbeastTableImplTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  def createQbeastTableImpl(tmpDir: String): QbeastTableImpl = {
    new QbeastTableImpl(
      TableIdentifier("students"),
      new Path(tmpDir),
      Map.empty,
      Some(schema),
      None,
      indexedTableFactory)

  }

  "QbeastTableImpl" should "load the schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.schema() shouldBe schema

    })

  it should "get the name" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {

      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.name() shouldBe "students"

    })

  it should "load capabilities" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {
      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.capabilities() shouldBe Set(
        ACCEPT_ANY_SCHEMA,
        BATCH_READ,
        V1_BATCH_WRITE,
        OVERWRITE_BY_FILTER,
        TRUNCATE).asJava
    })

  it should "create a QbeastWriteBuilder" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {
      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.newWriteBuilder(fakeLogicalWriteInfo) shouldBe a[QbeastWriteBuilder]
    })

  it should "load properties" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {
      val properties = Map("provider" -> "qbeast", "columnsToIndex" -> "id")
      val qbeastTableImpl = new QbeastTableImpl(
        TableIdentifier("students"),
        new Path(tmpDir),
        properties,
        Some(schema),
        None,
        indexedTableFactory)

      val qbeastTableProperties = qbeastTableImpl.properties()

      qbeastTableProperties.get("provider") shouldBe "qbeast"
      qbeastTableProperties.get("columnsToIndex") shouldBe "id"

    })

  it should "load metadata from the Catalog" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val qbeastCatalog = createQbeastCatalog(spark)
      val identifier = Identifier.of(defaultNamespace, "students")
      val tableIdentifier = TableIdentifier(identifier.name(), identifier.namespace().headOption)
      val properties = Map.empty[String, String]
      qbeastCatalog.createTable(identifier, columns, Array.empty[Transform], properties.asJava)

      val qbeastTableImpl = new QbeastTableImpl(
        tableIdentifier,
        new Path(tmpDir),
        properties,
        None,
        None,
        indexedTableFactory)

      qbeastTableImpl.schema() shouldBe schema
      qbeastTableImpl.v1Table shouldBe spark.sessionState.catalog.getTableMetadata(
        tableIdentifier)
    })

}
