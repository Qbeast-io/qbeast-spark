/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.concurrent.TimeLimits
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}

import scala.util.Random

/**
 * Speed tests for CubeId.
 */
class CubeIdSpeedTest extends AnyFlatSpec with Matchers with TimeLimits {
  private val timeout: Span = Span(100000, Millis)
  private val count = 1000000
  private val depth = 20

  private def createPoints(dimensionCount: Int): Seq[Point] = Seq.fill(count) {
    Point(Vector.fill(dimensionCount) { Random.nextDouble() })
  }

  private def createCoordinates(dimensionCount: Int): Seq[Array[Double]] = Seq.fill(count) {
    Array.fill(dimensionCount) { Random.nextDouble() }
  }

  // warm up
  createPoints(4).foreach(CubeId.containers(_).drop(depth))
  createPoints(3).foreach(CubeId.containers(_).drop(depth))
  createCoordinates(4).foreach(CubeKeyFactoryJava.createCubeKey(_, depth, '0'))
  createCoordinates(3).foreach(CubeKeyFactoryJava.createCubeKey(_, depth, '0'))

  "the time to run a test" should "should be very little" in {
    val points = createPoints(4)
    val start = System.currentTimeMillis()
    for (_ <- points) {}
    val time = System.currentTimeMillis() - start
    // scalastyle:off println
    println(s"Completed in $time msseconds - ${count.toDouble / time * 1000} IOPS")
  }

  "CubeKeyFactoryJava 4D test" should "complete in time" in {
    val coordinates = createCoordinates(4)
    val start = System.currentTimeMillis()
    failAfter(timeout) {
      for (point <- coordinates) {
        CubeKeyFactoryJava.createCubeKey(point, depth, '0')
      }
    }
    val time = System.currentTimeMillis() - start
    // scalastyle:off println
    println(s"4DJava completed in $time msseconds - ${count.toDouble / time * 1000} IOPS")
  }

  "CubeKeyFactoryJava 3D test" should "should be coherent in a from to scenario" in {
    val coordinates = createCoordinates(3)
    val start = System.currentTimeMillis()
    failAfter(timeout) {
      for (point <- coordinates) {
        CubeKeyFactoryJava.createCubeKey(point, depth, '0')
      }
    }
    val time = System.currentTimeMillis() - start
    // scalastyle:off println
    println(s"3DJava completed in $time msseconds - ${count.toDouble / time * 1000} IOPS")
  }

  "Cube 4D test" should "complete in time" in {
    val points = createPoints(4)
    val start = System.currentTimeMillis()
    failAfter(timeout) {
      points.foreach(CubeId.containers(_).drop(depth))
    }
    val time = System.currentTimeMillis() - start
    // scalastyle:off println
    println(s"4D completed in $time msseconds - ${count.toDouble / time * 1000} IOPS")
  }

  "Cube 3D test" should "should be coherent in a from to scenario" in {
    val points = createPoints(3)
    val start = System.currentTimeMillis()
    failAfter(timeout) {
      points.foreach(CubeId.containers(_).drop(depth))
    }
    val time = System.currentTimeMillis() - start
    // scalastyle:off println
    println(s"3D completed in $time msseconds - ${count.toDouble / time * 1000} IOPS")
  }

  "The sorting of CubeId" should "be faster than comparing using strings " in {
    val root = CubeId.root(10)
    val oneMillion = Random.shuffle(root.children.flatMap(_.children)).toVector
    val t1 = System.currentTimeMillis()
    val s1 = oneMillion.sorted
    println(System.currentTimeMillis() - t1)

    val t2 = System.currentTimeMillis()
    val s2 = oneMillion.sortBy(_.string)
    println(System.currentTimeMillis() - t2)
    println(s"The first is ${s1.head} and the second ${s2.head}")
    s1.head shouldBe s2.head
  }
}
