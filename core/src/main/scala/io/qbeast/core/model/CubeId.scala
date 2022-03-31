/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.core.model.CubeId.{ChildrenIterator, Codec}

import java.nio.ByteBuffer
import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
 * CubeId companion object.
 */
object CubeId {

  /**
   * Creates a cube identifier for given dimension count and bytes.
   *
   * @param dimensionCount the dimension count
   * @param bytes the bytes
   * @return a cube identifier
   */
  def apply(dimensionCount: Int, bytes: Array[Byte]): CubeId = {
    require(dimensionCount > 0)
    require(bytes.length >= Integer.BYTES)
    val buffer = ByteBuffer.wrap(bytes)
    val depth = buffer.getInt
    val bitMask = Array.newBuilder[Long]
    while (buffer.remaining() >= java.lang.Long.BYTES) {
      bitMask += buffer.getLong
    }
    new CubeId(dimensionCount, depth, bitMask.result())
  }

  /**
   * Creates a cube identifier for given dimension count and string.
   *
   * @param dimensionCount the dimension count
   * @param string the string
   * @return a cube identifier
   */
  def apply(dimensionCount: Int, string: String): CubeId =
    Codec.decode(dimensionCount, string)

  /**
   * Creates a root cube identifier for given dimension count.
   *
   * @param dimensionCount the dimension count
   * @return a new root cube identifier
   */
  def root(dimensionCount: Int): CubeId = {
    require(dimensionCount > 0)
    new CubeId(dimensionCount, 0, Array(0L))
  }

  /**
   * Returns the identifiers of the cubes which contain a given point. The returned
   * iterator allows to traverse the sequence of nested container cubes starting
   * with the root cube.
   *
   * @param point the point
   * @return the identifiers of the container cubes
   */
  def containers(point: Point): Iterator[CubeId] = {
    require(point.coordinates.forall { c => 0.0 <= c && c <= 1.0 })
    new ContainersIterator(point, None)
  }

  /**
   * Returns the identifiers of the cubes which contain a given point. The returned
   * iterator allows to traverse the sequence of nested container cubes starting
   * with the child cube of the specified parent cube.
   *
   * @param point the point
   * @param parent the parent cube identifier
   * @return the identifiers of the container cubes
   */
  def containers(point: Point, parent: CubeId): Iterator[CubeId] = {
    require(parent.contains(point))
    new ContainersIterator(point, Some(parent))
  }

  /**
   * Returns the identifier of the the cube containing a
   * given point and having the specified depth.
   *
   * @param point the point
   * @param depth the depth
   * @return the container cube identifier
   */
  def container(point: Point, depth: Int): CubeId = {
    require(depth >= 0)
    containers(point).drop(depth).next()
  }

  private class ContainersIterator(point: Point, parent: Option[CubeId])
      extends Iterator[CubeId] {

    private val (coordinates, parentDepth, bits) = parent match {
      case Some(cubeId) =>
        val shiftedCoordinates = point.coordinates.toArray
        if (cubeId.depth > 0) {
          val factor = math.pow(2.0, cubeId.depth)
          for (i <- shiftedCoordinates.indices) {
            var x = shiftedCoordinates(i)
            if (x < 1) {
              x *= factor
              x -= math.floor(x)
              shiftedCoordinates.update(i, x)
            }
          }
        }
        (shiftedCoordinates, cubeId.depth, mutable.BitSet.fromBitMask(cubeId.bitMask))
      case None =>
        (point.coordinates.toArray, -1, mutable.BitSet.empty)
    }

    private val dimensionCount = coordinates.length
    private var depth = parentDepth

    override def hasNext: Boolean = true

    override def next(): CubeId = {
      if (depth >= 0) {
        val offset = dimensionCount * depth
        for (i <- coordinates.indices) {
          var x = coordinates(i) * 2
          if (x >= 1) {
            bits += offset + i
            x -= 1
          }
          coordinates.update(i, x)
        }
      }
      depth += 1
      new CubeId(dimensionCount, depth, bits.toBitMask)
    }

  }

  private class ChildrenIterator(parent: CubeId) extends Iterator[CubeId] {
    private var child: Option[CubeId] = Some(parent.firstChild)

    override def hasNext: Boolean = child.nonEmpty

    override def next(): CubeId = child match {
      case Some(value) =>
        child = value.nextSibling
        value
      case None =>
        throw new NoSuchElementException("The next child is not available.")
    }

  }

  private object Codec {

    private val symbols =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toArray

    def encode(cubeId: CubeId): String = {
      val dimensionCount = cubeId.dimensionCount
      val depth = cubeId.depth
      val bitMask = BitSet.fromBitMaskNoCopy(cubeId.bitMask)
      val capacity = ((dimensionCount + 5) / 6) * depth
      val builder = new StringBuilder(capacity)
      for (i <- 0 until depth) {
        var begin = dimensionCount * i
        val end = begin + dimensionCount
        while (begin < end) {
          builder += encodeSixBits(bitMask, begin, end)
          begin += 6
        }
      }
      builder.result()
    }

    def decode(dimensionCount: Int, string: String): CubeId = {
      if (string.isEmpty) {
        return CubeId.root(dimensionCount)
      }
      val symbolCount = (dimensionCount + 5) / 6
      val depth = string.length / symbolCount
      val bitMask = mutable.BitSet.empty
      for (i <- 0 until depth) {
        var begin = dimensionCount * i
        val end = begin + dimensionCount
        var j = symbolCount * i
        while (begin < end) {
          decodeSixBits(string(j), begin, end, bitMask)
          begin += 6
          j += 1
        }
      }
      new CubeId(dimensionCount, depth, bitMask.toBitMask)
    }

    private def encodeSixBits(bitMask: BitSet, begin: Int, end: Int): Char = {
      var index = 0
      for (i <- 0 until math.min(6, end - begin)) {
        if (bitMask.contains(begin + i)) {
          index |= 1 << (5 - i)
        }
      }
      symbols(index)
    }

    private def decodeSixBits(
        symbol: Char,
        begin: Int,
        end: Int,
        bitMask: mutable.BitSet): Unit = {
      val index = if ('A' <= symbol && symbol <= 'Z') {
        symbol - 'A'
      } else if ('a' <= symbol && symbol <= 'z') {
        26 + symbol - 'a'
      } else if ('0' <= symbol && symbol <= '9') {
        52 + symbol - '0'
      } else if (symbol == '+') {
        62
      } else if (symbol == '/') {
        63
      } else {
        throw new IllegalArgumentException(s"Invalid symbol '$symbol'.")
      }
      for (i <- 0 until math.min(6, end - begin)) {
        val mask = 1 << (5 - i)
        if ((index & mask) == mask) {
          bitMask += begin + i
        }
      }
    }

  }

}

/**
 * Cube identifier.
 *
 * @param dimensionCount the dimension count
 * @param depth the cube depth
 * @param bitMask the bitMask representing the cube z-index.
 */
case class CubeId(dimensionCount: Int, depth: Int, bitMask: Array[Long])
    extends Serializable
    with Ordered[CubeId] {
  private lazy val range = getRange

  /**
   * Compare two CubeIds.
   * @param that the other CubeId
   * @return a negative integer, zero, or a positive integer as this CubeId
   *         is less than, equal to, or greater than the other CubeId.
   */
  override def compare(that: CubeId): Int = {
    val thisBitset = BitSet.fromBitMaskNoCopy(bitMask)
    val thatBitset = BitSet.fromBitMaskNoCopy(that.bitMask)
    val commonDepth = math.min(depth, that.depth)
    for (depthOffset <- 0.until(commonDepth * dimensionCount)) {
      val firstBit = thisBitset.contains(depthOffset)
      val secondBit = thatBitset.contains(depthOffset)
      if (firstBit != secondBit) {
        if (firstBit) {
          return 1
        } else {
          return -1
        }
      }

    }
    // We end up here, if one of the 2 cubes is an ancestor of the other.
    // If positive, that < this => this is of deeper level
    // If negative, that > this => that is of deeper level
    // If equal, both are of the same level
    depth.compare(that.depth)
  }

  /**
   * Returns true if this cube is the ancestor of the other cube.
   * In case this and other are the same cube, it returns true.
   * @param other cube to check
   * @return
   */
  def isAncestorOf(other: CubeId): Boolean = {
    require(
      other.dimensionCount == dimensionCount,
      "The two cubes must have the same dimension count.")

    if (depth > other.depth) {
      false
    } else {
      val end = dimensionCount * depth
      val possibleDescendantBits = BitSet.fromBitMaskNoCopy(other.bitMask).until(end).toBitMask

      for (i <- possibleDescendantBits.indices) {
        if (possibleDescendantBits(i) != bitMask(i)) {
          return false
        }
      }
      true
    }
  }

  /**
   * Returns whether the identifier represents the root cube.
   *
   * @return the identifier represents the root cube.
   */
  def isRoot: Boolean = depth == 0

  /**
   * Returns the parent cube identifier if available.
   *
   * @return the parent cube identifier if available.
   */
  def parent: Option[CubeId] = {
    if (isRoot) {
      return None
    }
    val parentDepth = depth - 1
    val end = dimensionCount * parentDepth
    val bits = BitSet.fromBitMaskNoCopy(bitMask).until(end)
    Some(new CubeId(dimensionCount, parentDepth, bits.toBitMask))
  }

  /**
   * Returns the first child identifier.
   *
   * @return the first child identifier
   */
  def firstChild: CubeId = {
    new CubeId(dimensionCount, depth + 1, bitMask)
  }

  /**
   * Returns the child identifiers.
   *
   * @return the child identifiers
   */
  def children: Iterator[CubeId] = new ChildrenIterator(this)

  /**
   * Returns the next sibling identifier if available.
   *
   * @return the next sibling identifier if available
   */
  def nextSibling: Option[CubeId] = {
    if (isRoot) {
      return None
    }
    val from = dimensionCount * (depth - 1)
    val end = from + dimensionCount
    val bits = mutable.BitSet.fromBitMask(bitMask)
    for (i <- (from until end).reverse) {
      if (bits.contains(i)) {
        bits -= i
      } else {
        bits += i
        return Some(new CubeId(dimensionCount, depth, bits.toBitMask))
      }
    }
    None
  }

  /**
   * Returns the cube point with minimum coordinates.
   *
   * @return the cube point with minimum coordinates.
   */
  def from: Point = range._1

  /**
   * Returns the cube point with maximum coordinates.
   *
   * @return the cube point with maximum coordinates.
   */
  def to: Point = range._2

  /**
   * Returns whether the cube contains a given point.
   *
   * @param point the point
   * @return the cube contains the point
   */
  private[model] def contains(point: Point): Boolean = {
    require(point.dimensionCount == dimensionCount)
    from.coordinates.zip(point.coordinates).zip(to.coordinates).forall { case ((f, p), t) =>
      f <= p && (p < t || (p == t && t == 1.0))
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: CubeId =>
      dimensionCount == other.dimensionCount && depth == other.depth && bitMask.sameElements(
        other.bitMask)
    case _ => false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + dimensionCount
    result = prime * result + depth
    result = prime * result + bitMask.toSeq.hashCode()
    result
  }

  /**
   * Returns the bytes to be stored in a byte array.
   *
   * @return the bytes
   */
  def bytes: Array[Byte] = {
    val length = Integer.BYTES + java.lang.Long.BYTES * bitMask.length
    val result = new Array[Byte](length)
    val buffer = ByteBuffer.wrap(result)
    buffer.putInt(depth)
    bitMask.foreach(buffer.putLong)
    result
  }

  /**
   * Returns the string to be stored in the block file metadata.
   *
   * @return the string
   */
  def string: String = Codec.encode(this)

  private def getRange: (Point, Point) = {
    val bits = BitSet.fromBitMaskNoCopy(bitMask)
    val from = Array.fill(dimensionCount)(0.0)
    for (i <- (0 until depth).reverse) {
      val offset = dimensionCount * i
      for (j <- from.indices) {
        var x = from(j)
        if (bits.contains(offset + j)) {
          x += 1
        }
        x *= 0.5
        from.update(j, x)
      }
    }
    val width = math.pow(0.5, depth)
    val to = from.map(_ + width)
    (Point(from.toIndexedSeq), Point(to.toIndexedSeq))
  }

  override def toString: String = s"CubeId($dimensionCount, $depth, $string)"

}
