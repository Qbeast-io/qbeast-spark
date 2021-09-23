/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import org.apache.spark.sql.types.{
  BinaryType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[QbeastColumns]].
 */
class QbeastColumnsTest extends AnyFlatSpec with Matchers {
  "QbeastColumns" should "define column names starting with _qbeast" in {
    QbeastColumns.weightColumnName should startWith("_qbeast")
    QbeastColumns.cubeColumnName should startWith("_qbeast")
    QbeastColumns.stateColumnName should startWith("_qbeast")
    QbeastColumns.revisionColumnName should startWith("_qbeast")
    QbeastColumns.cubeToReplicateColumnName should startWith("_qbeast")
  }

  it should "create instance from schema correctly" in {
    val schema1 = StructType(
      Seq(
        StructField("A", StringType),
        StructField(QbeastColumns.weightColumnName, IntegerType),
        StructField("B", StringType),
        StructField(QbeastColumns.cubeColumnName, BinaryType),
        StructField("C", StringType),
        StructField(QbeastColumns.stateColumnName, StringType)))
    val columns1 = QbeastColumns(schema1)
    columns1.weightColumnIndex shouldBe 1
    columns1.hasWeightColumn shouldBe true
    columns1.cubeColumnIndex shouldBe 3
    columns1.hasCubeColumn shouldBe true
    columns1.stateColumnIndex shouldBe 5
    columns1.hasStateColumn shouldBe true
    columns1.revisionColumnIndex shouldBe -1
    columns1.hasRevisionColumn shouldBe false
    columns1.cubeToReplicateColumnIndex shouldBe -1
    columns1.hasCubeToReplicateColumn shouldBe false

    val schema2 = StructType(
      Seq(
        StructField("A", StringType),
        StructField(QbeastColumns.revisionColumnName, LongType),
        StructField("B", StringType),
        StructField(QbeastColumns.cubeToReplicateColumnName, BinaryType),
        StructField("C", StringType)))
    val columns2 = QbeastColumns(schema2)
    columns2.weightColumnIndex shouldBe -1
    columns2.hasWeightColumn shouldBe false
    columns2.cubeColumnIndex shouldBe -1
    columns2.hasCubeColumn shouldBe false
    columns2.stateColumnIndex shouldBe -1
    columns2.hasStateColumn shouldBe false
    columns2.revisionColumnIndex shouldBe 1
    columns2.hasRevisionColumn shouldBe true
    columns2.cubeToReplicateColumnIndex shouldBe 3
    columns2.hasCubeToReplicateColumn shouldBe true
  }

  it should "implement contains correctly" in {
    val schema1 = StructType(
      Seq(
        StructField("A", StringType),
        StructField(QbeastColumns.weightColumnName, IntegerType),
        StructField("B", StringType),
        StructField(QbeastColumns.cubeColumnName, BinaryType),
        StructField("C", StringType),
        StructField(QbeastColumns.stateColumnName, StringType)))
    val columns1 = QbeastColumns(schema1)
    columns1.contains(0) shouldBe false
    columns1.contains(1) shouldBe true
    columns1.contains(2) shouldBe false
    columns1.contains(3) shouldBe true
    columns1.contains(4) shouldBe false
    columns1.contains(5) shouldBe true

    val schema2 = StructType(
      Seq(
        StructField("A", StringType),
        StructField(QbeastColumns.revisionColumnName, LongType),
        StructField("B", StringType),
        StructField(QbeastColumns.cubeToReplicateColumnName, BinaryType),
        StructField("C", StringType)))
    val columns2 = QbeastColumns(schema2)
    columns2.contains(0) shouldBe false
    columns2.contains(1) shouldBe true
    columns2.contains(2) shouldBe false
    columns2.contains(3) shouldBe true
    columns2.contains(4) shouldBe false
  }

  it should "recognize all the Qbeast column names" in {
    QbeastColumns.contains(QbeastColumns.weightColumnName) shouldBe true
    QbeastColumns.contains(QbeastColumns.cubeColumnName) shouldBe true
    QbeastColumns.contains(QbeastColumns.stateColumnName) shouldBe true
    QbeastColumns.contains(QbeastColumns.revisionColumnName) shouldBe true
    QbeastColumns.contains(QbeastColumns.cubeToReplicateColumnName) shouldBe true
    QbeastColumns.contains("weight") shouldBe false
    QbeastColumns.contains("cube") shouldBe false
    QbeastColumns.contains("state") shouldBe false
    QbeastColumns.contains("revision") shouldBe false
    QbeastColumns.contains("cubeToReplicate") shouldBe false
  }

  it should "recognize the Qbeast column fields" in {
    QbeastColumns.contains(StructField(QbeastColumns.weightColumnName, IntegerType)) shouldBe true
    QbeastColumns.contains(StructField(QbeastColumns.cubeColumnName, BinaryType)) shouldBe true
    QbeastColumns.contains(StructField(QbeastColumns.stateColumnName, StringType)) shouldBe true
    QbeastColumns.contains(StructField(QbeastColumns.revisionColumnName, LongType)) shouldBe true
    QbeastColumns.contains(
      StructField(QbeastColumns.cubeToReplicateColumnName, BinaryType)) shouldBe true
    QbeastColumns.contains(StructField("weight", IntegerType)) shouldBe false
    QbeastColumns.contains(StructField("cube", BinaryType)) shouldBe false
    QbeastColumns.contains(StructField("state", StringType)) shouldBe false
    QbeastColumns.contains(StructField("revision", LongType)) shouldBe false
    QbeastColumns.contains(StructField("cubeToReplicate", BinaryType)) shouldBe false
  }
}
