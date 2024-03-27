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

import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class CreateCubeKeySpeedTest extends AnyWordSpec with Matchers with TimeLimitedTests {

  val timeLimit: Span =
    Span(7 + 10 + 10000, Millis) // equal to 1M IOPS (7 ms is the time required by the test
  private val times = 10000

  private val points4d = 1
    .to(times)
    .map(_ =>
      Point(Random.nextDouble(), Random.nextDouble(), Random.nextDouble(), Random.nextDouble()))
    .toVector

  private val points3d = 1
    .to(times)
    .map(_ => Point(Random.nextDouble(), Random.nextDouble(), Random.nextDouble()))
    .toVector

  private val points4dJ = 1
    .to(times)
    .map(_ =>
      Array(Random.nextDouble(), Random.nextDouble(), Random.nextDouble(), Random.nextDouble()))
    .toVector

  private val points3dJ = 1
    .to(times)
    .map(_ => Array(Random.nextDouble(), Random.nextDouble(), Random.nextDouble()))
    .toVector

  // warming up
  for (from <- points4d) {

    CubeKeyFactory.createCubeKey(from, 20)
  }

  for (from <- points3d) {

    CubeKeyFactory.createCubeKey(from, 20)
  }

  "the time to run a test" should {
    "should be very little" in {
      val starting = System.currentTimeMillis()
      for (_ <- points4d) {}
      val time = System.currentTimeMillis() - starting
      // scalastyle:off println
      println(s"Completed in $time milliseconds - ${times.toDouble / time * 1000} IOPS")

    }
  }

  "A 4D CubeKeyJava" should {
    "complete in time" in {
      val starting = System.currentTimeMillis()
      for (from <- points4dJ) {

        CubeKeyFactoryJava.createCubeKey(from, 20, '0')
      }
      val time = System.currentTimeMillis() - starting
      // scalastyle:off println
      println(s"4DJava completed in $time milliseconds - ${times.toDouble / time * 1000} IOPS")

    }
  }

  "A 3D CubeKeyJava" should {
    "should be coherent in a from to scenario" in {
      val starting = System.currentTimeMillis()
      for (from <- points3dJ) {
        CubeKeyFactoryJava.createCubeKey(from, 20, '0')
      }
      val time = System.currentTimeMillis() - starting
      // scalastyle:off println
      println(s"3DJava completed in $time milliseconds - ${times.toDouble / time * 1000} IOPS")

    }
  }

  "A 4D CubeKey" should {
    "complete in time" in {

      val starting = System.currentTimeMillis()
      for (from <- points4d) {

        CubeKeyFactory.createCubeKey(from, 20)
      }
      val time = System.currentTimeMillis() - starting
      // scalastyle:off println
      println(s"4D completed in $time milliseconds - ${times.toDouble / time * 1000} IOPS")

    }
  }

  "A 3D CubeKey" should {
    "should be coherent in a from to scenario" in {

      val starting = System.currentTimeMillis()
      for (from <- points3d) {
        CubeKeyFactory.createCubeKey(from, 20)
      }
      val time = System.currentTimeMillis() - starting
      // scalastyle:off println
      println(s"3D completed in $time milliseconds - ${times.toDouble / time * 1000} IOPS")

    }
  }

}
