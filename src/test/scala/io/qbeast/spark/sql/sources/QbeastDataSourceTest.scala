/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.sources

import io.qbeast.spark.table.{IndexedTable, IndexedTableFactory}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.mockito.ArgumentMatchers.{any, anyBoolean}
import org.mockito.Mockito.{verify, when}
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.Collections
import scala.collection.JavaConverters._

/**
 * Tests for QbeastDataSource.
 */
class QbeastDataSourceTest extends FixtureAnyFlatSpec with MockitoSugar with Matchers {
  private val path = "path"
  private val columnsToIndex = List("foo", "bar")

  case class Fixture(
      sqlContext: SQLContext,
      relation: BaseRelation,
      table: IndexedTable,
      tableFactory: IndexedTableFactory,
      dataSource: QbeastDataSource)

  override type FixtureParam = Fixture

  override protected def withFixture(test: OneArgTest): Outcome = {
    val sqlContext = mock[SQLContext]
    val relation = mock[BaseRelation]

    val table = mock[IndexedTable]
    when(table.load()).thenReturn(relation)
    when(table.save(any[DataFrame], any[List[String]], anyBoolean())).thenReturn(relation)

    val tableFactory = mock[IndexedTableFactory]
    when(tableFactory.getIndexedTable(sqlContext, path)).thenReturn(table)

    val dataSource = new QbeastDataSource(tableFactory)

    val fixture = Fixture(sqlContext, relation, table, tableFactory, dataSource)
    withFixture(test.toNoArgTest(fixture))
  }

  "QbeastDataSource" should "return 'qbeast' as the table name" in { f =>
    f.dataSource.shortName() shouldBe "qbeast"
  }

  it should "infer empty schema" in { f =>
    val options = CaseInsensitiveStringMap.empty
    f.dataSource.inferSchema(options) shouldBe StructType(Seq())
  }

  it should "return correct table" in { f =>
    val schema = StructType(Seq())
    val partitioning = Array.empty[Transform]
    val properties = Collections.emptyMap[String, String]()
    val table = f.dataSource.getTable(schema, partitioning, properties)
    table.schema() shouldBe schema
    table.capabilities() shouldBe Set(
      ACCEPT_ANY_SCHEMA,
      BATCH_READ,
      V1_BATCH_WRITE,
      OVERWRITE_BY_FILTER,
      TRUNCATE).asJava
  }

  it should "append data frame if mode is Append" in { f =>
    val parameters = Map("path" -> path, "columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    f.dataSource.createRelation(
      f.sqlContext,
      SaveMode.Append,
      parameters,
      data) shouldBe f.relation
    verify(f.table).save(data, columnsToIndex, append = true)
  }

  it should "overwrite table if mode is Overwrite" in { f =>
    val parameters = Map("path" -> path, "columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    f.dataSource.createRelation(
      f.sqlContext,
      SaveMode.Overwrite,
      parameters,
      data) shouldBe f.relation
    verify(f.table).save(data, columnsToIndex, append = false)
  }

  it should "throw exception if mode is ErrorIfExists and the table exists" in { f =>
    val parameters = Map("path" -> path, "columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    when(f.table.exists).thenReturn(true)
    a[AnalysisException] shouldBe thrownBy {
      f.dataSource.createRelation(f.sqlContext, SaveMode.ErrorIfExists, parameters, data)
    }
  }

  it should "write table if mode is ErrorIfExists and the table does not exist" in { f =>
    val parameters = Map("path" -> path, "columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    when(f.table.exists).thenReturn(false)
    f.dataSource.createRelation(
      f.sqlContext,
      SaveMode.ErrorIfExists,
      parameters,
      data) shouldBe f.relation
    verify(f.table).save(data, columnsToIndex, append = false)
  }

  it should "return relation if mode is Ignore and the table exists" in { f =>
    val parameters = Map("path" -> path, "columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    when(f.table.exists).thenReturn(true)
    f.dataSource.createRelation(
      f.sqlContext,
      SaveMode.Ignore,
      parameters,
      data) shouldBe f.relation
    verify(f.table).load()
  }

  it should "write table if mode is Ignore and the table does not exist" in { f =>
    val parameters = Map("path" -> path, "columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    when(f.table.exists).thenReturn(false)
    f.dataSource.createRelation(
      f.sqlContext,
      SaveMode.Ignore,
      parameters,
      data) shouldBe f.relation
    verify(f.table).save(data, columnsToIndex, append = false)
  }

  it should "throw exception for write if path is not specified" in { f =>
    val parameters = Map("columnsToIndex" -> columnsToIndex.mkString(","))
    val data = mock[DataFrame]
    a[AnalysisException] shouldBe thrownBy {
      f.dataSource.createRelation(f.sqlContext, SaveMode.Append, parameters, data)
    }
  }

  it should "throw exception for write if columns to index are not specified" in { f =>
    val parameters = Map("path" -> path)
    val data = mock[DataFrame]
    a[AnalysisException] shouldBe thrownBy {
      f.dataSource.createRelation(f.sqlContext, SaveMode.Append, parameters, data)
    }
  }

  it should "throw exception for createRelation if the table does not exist" in { f =>
    val parameters = Map("path" -> path)
    a[AnalysisException] shouldBe thrownBy {
      f.dataSource.createRelation(f.sqlContext, parameters)
    }
  }

  it should "throw exception for read if the path is not specified" in { f =>
    a[AnalysisException] shouldBe thrownBy {
      f.dataSource.createRelation(f.sqlContext, Map.empty)
    }
  }
}
