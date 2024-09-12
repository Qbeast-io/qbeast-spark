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
package io.qbeast.core.model

import org.scalatest.concurrent.TimeLimits
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Span

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
    val oneMillion =
      Random.shuffle(root.children.flatMap(child => Seq(child) ++ child.children)).toVector
    val start2 = System.currentTimeMillis()
    val s2 = oneMillion.sortBy(_.string)
    val time2 = System.currentTimeMillis() - start2
    println(s"Time taken to sort by string: ${time2}")

    val start1 = System.currentTimeMillis()
    val s1 = oneMillion.sorted
    val time1 = System.currentTimeMillis() - start1
    println(s"Time spent on oneMillion.sorted: ${time1}")

    println(s"s1: ${s1.head}, s2: ${s2.head}")
    time2 should be > time1

  }

}
