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
package io.qbeast.spark.index

import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[QbeastColumns]].
 */
class QbeastColumnsTest extends AnyFlatSpec with Matchers {

  "QbeastColumns" should "define column names starting with _qbeast" in {
    QbeastColumns.weightColumnName should startWith("_qbeast")
    QbeastColumns.cubeColumnName should startWith("_qbeast")
    QbeastColumns.fileUUIDColumnName should startWith("_qbeast")
    QbeastColumns.filenameColumnName should startWith("_qbeast")
  }

  it should "create instance from schema correctly" in {
    val schema1 = StructType(
      Seq(
        StructField("A", StringType),
        StructField(QbeastColumns.weightColumnName, IntegerType),
        StructField("B", StringType),
        StructField(QbeastColumns.cubeColumnName, BinaryType),
        StructField("C", StringType)))
    val columns1 = QbeastColumns(schema1)
    columns1.weightColumnIndex shouldBe 1
    columns1.hasWeightColumn shouldBe true
    columns1.cubeColumnIndex shouldBe 3
    columns1.hasCubeColumn shouldBe true
    columns1.fileUUIDColumnIndex shouldBe -1
    columns1.hasFileUUIDColumn shouldBe false
    columns1.filenameColumnIndex shouldBe -1
    columns1.hasFilenameColumn shouldBe false

    val schema2 = StructType(
      Seq(
        StructField("A", StringType),
        StructField("B", StringType),
        StructField("C", StringType),
        StructField(QbeastColumns.fileUUIDColumnName, BinaryType),
        StructField("D", StringType),
        StructField(QbeastColumns.filenameColumnName, StringType)))
    val columns2 = QbeastColumns(schema2)
    columns2.weightColumnIndex shouldBe -1
    columns2.hasWeightColumn shouldBe false
    columns2.cubeColumnIndex shouldBe -1
    columns2.hasCubeColumn shouldBe false
    columns2.fileUUIDColumnIndex shouldBe 3
    columns2.hasFileUUIDColumn shouldBe true
    columns2.filenameColumnIndex shouldBe 5
    columns2.hasFilenameColumn shouldBe true
  }

  it should "implement contains correctly" in {
    val schema1 = StructType(
      Seq(
        StructField("A", StringType),
        StructField(QbeastColumns.weightColumnName, IntegerType),
        StructField("B", StringType),
        StructField(QbeastColumns.cubeColumnName, BinaryType),
        StructField("C", StringType)))
    val columns1 = QbeastColumns(schema1)
    columns1.contains(0) shouldBe false
    columns1.contains(1) shouldBe true
    columns1.contains(2) shouldBe false
    columns1.contains(3) shouldBe true
    columns1.contains(4) shouldBe false

    val schema2 = StructType(
      Seq(
        StructField("A", StringType),
        StructField("B", StringType),
        StructField("C", StringType),
        StructField(QbeastColumns.fileUUIDColumnName, BinaryType)))
    val columns2 = QbeastColumns(schema2)
    columns2.contains(0) shouldBe false
    columns2.contains(1) shouldBe false
    columns2.contains(2) shouldBe false
    columns2.contains(3) shouldBe true
  }

  it should "recognize all the Qbeast column names" in {
    QbeastColumns.contains(QbeastColumns.weightColumnName) shouldBe true
    QbeastColumns.contains(QbeastColumns.cubeColumnName) shouldBe true
    QbeastColumns.contains(QbeastColumns.fileUUIDColumnName) shouldBe true
    QbeastColumns.contains("weight") shouldBe false
    QbeastColumns.contains("cube") shouldBe false
  }

  it should "recognize the Qbeast column fields" in {
    QbeastColumns.contains(StructField(QbeastColumns.weightColumnName, IntegerType)) shouldBe true
    QbeastColumns.contains(StructField(QbeastColumns.cubeColumnName, BinaryType)) shouldBe true
    QbeastColumns.contains(
      StructField(QbeastColumns.fileUUIDColumnName, BinaryType)) shouldBe true
    QbeastColumns.contains(StructField("weight", IntegerType)) shouldBe false
    QbeastColumns.contains(StructField("cube", BinaryType)) shouldBe false
  }

}
