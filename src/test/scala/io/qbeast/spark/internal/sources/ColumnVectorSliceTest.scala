/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.StructField
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class ColumnVectorSliceTest extends AnyFlatSpec with Matchers {

  "ColumnVectorSlice" should "handle booleans correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.BooleanType)
    target.putBoolean(0, false)
    target.putBoolean(1, true)
    target.putBoolean(2, false)
    target.putBoolean(3, true)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getBoolean(0) shouldBe true
    slice.getBoolean(1) shouldBe false
  }

  it should "handle bytes correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.ByteType)
    target.putByte(0, 0)
    target.putByte(1, 1)
    target.putByte(2, 2)
    target.putByte(3, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getByte(0) shouldBe 1
    slice.getByte(1) shouldBe 2
    slice.getBytes(0, 1) shouldBe Array[Byte](1)
  }

  it should "handle shorts correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.ShortType)
    target.putShort(0, 0)
    target.putShort(1, 1)
    target.putShort(2, 2)
    target.putShort(3, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getShort(0) shouldBe 1
    slice.getShort(1) shouldBe 2
    slice.getShorts(0, 1) shouldBe Array[Short](1)
  }

  it should "handle ints correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.IntegerType)
    target.putInt(0, 0)
    target.putInt(1, 1)
    target.putInt(2, 2)
    target.putInt(3, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getInt(0) shouldBe 1
    slice.getInt(1) shouldBe 2
    slice.getInts(0, 1) shouldBe Array[Int](1)
  }

  it should "handle longs correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.LongType)
    target.putLong(0, 0)
    target.putLong(1, 1)
    target.putLong(2, 2)
    target.putLong(3, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getLong(0) shouldBe 1
    slice.getLong(1) shouldBe 2
    slice.getLongs(0, 1) shouldBe Array[Long](1)
  }

  it should "handle floats correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.FloatType)
    target.putFloat(0, 0)
    target.putFloat(1, 1)
    target.putFloat(2, 2)
    target.putFloat(3, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getFloat(0) shouldBe 1
    slice.getFloat(1) shouldBe 2
    slice.getFloats(0, 1) shouldBe Array[Float](1)
  }

  it should "handle doubles correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.DoubleType)
    target.putDouble(0, 0)
    target.putDouble(1, 1)
    target.putDouble(2, 2)
    target.putDouble(3, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getDouble(0) shouldBe 1
    slice.getDouble(1) shouldBe 2
    slice.getDoubles(0, 1) shouldBe Array[Double](1)
  }

  it should "handle arrays correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.createArrayType(DataTypes.IntegerType))
    target.putArray(0, 0, 0)
    target.putArray(1, 0, 1)
    target.putArray(2, 0, 2)
    target.putArray(3, 0, 3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getArray(0).numElements() shouldBe 1
    slice.getArray(1).numElements() shouldBe 2
  }

  it should "handle nulls correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.IntegerType)
    target.putInt(0, 0)
    target.putNull(1)
    target.putInt(2, 2)
    target.putNull(3)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.numNulls() shouldBe 1
    slice.hasNull() shouldBe true
    slice.isNullAt(0) shouldBe true
    slice.isNullAt(1) shouldBe false
  }

  it should "handle decimals correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.createDecimalType())
    target.putDecimal(0, Decimal(0), 10)
    target.putDecimal(1, Decimal(1), 10)
    target.putDecimal(2, Decimal(2), 10)
    target.putDecimal(3, Decimal(3), 10)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getDecimal(0, 10, 0).toInt shouldBe 1
    slice.getDecimal(1, 10, 0).toInt shouldBe 2
  }

  it should "handle strings correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.StringType)
    target.putByteArray(0, "0".getBytes(StandardCharsets.UTF_8))
    target.putByteArray(1, "1".getBytes(StandardCharsets.UTF_8))
    target.putByteArray(2, "2".getBytes(StandardCharsets.UTF_8))
    target.putByteArray(3, "3".getBytes(StandardCharsets.UTF_8))

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getUTF8String(0).toString() shouldBe "1"
    slice.getUTF8String(1).toString() shouldBe "2"
  }

  it should "handle binaries correctly" in {
    val target = new OnHeapColumnVector(4, DataTypes.BinaryType)
    target.putByteArray(0, Array[Byte](0))
    target.putByteArray(1, Array[Byte](1))
    target.putByteArray(2, Array[Byte](2))
    target.putByteArray(3, Array[Byte](3))

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getBinary(0)(0) shouldBe 1
    slice.getBinary(1)(0) shouldBe 2
  }

  it should "handle child vectors correctly" in {
    val target = new OnHeapColumnVector(
      4,
      DataTypes.createStructType(Array(StructField("0", DataTypes.BooleanType))))
    target.getChild(0).putBoolean(0, true)
    target.getChild(0).putBoolean(1, false)

    val slice = new ColumnVectorSlice(target, 1, 3)
    slice.getChild(0).getBoolean(0) shouldBe true
    slice.getChild(0).getBoolean(1) shouldBe false
  }
}
