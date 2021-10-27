/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

import org.apache.commons.lang3.StringUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class CreateCubeKeyTest extends AnyWordSpec with Matchers {

  "A 4D CubeKey" should {
    "should be coherent in a from to scenario" in {
      val from =
        Point(0.00, 0.35366827644197594, 0.36683283014651886, 0.38392387982921825)
      val to =
        Point(0.015, 0.36480719058864669, 0.37, 0.36783275092737092)
      val centralPoint = Point(.0107, .365, .368, .37)
      val qfrom = CubeKeyFactory.createCubeKey(from, 20)
      val qto = CubeKeyFactory.createCubeKey(to, 20)
      val qcenter = CubeKeyFactory.createCubeKey(centralPoint, 20)
      val qcommon = StringUtils.getCommonPrefix(qfrom, qto) // The query's MBR
      qcenter should startWith(qcommon)

    }
  }

  "A 3D CubeKey" should {
    "should be coherent in a from to scenario" in {

      val from = Point(0.00, 0.35366827644197594, 0.36683283014651886)
      val to = Point(0.015, 0.36980719058864669, 0.369123645959100985)
      val centralPoint = Point(.0107, .365, .368)

      val qfrom = CubeKeyFactory.createCubeKey(from, 20)
      val qto = CubeKeyFactory.createCubeKey(to, 20)
      val qcenter = CubeKeyFactory.createCubeKey(centralPoint, 20)
      val qcommon = StringUtils.getCommonPrefix(qfrom, qto) // The query's MBR

      qcenter should startWith(qcommon)

    }
  }

  "A 3D CubeKey" should {
    "always share the father prefix" in {

      val point = Point(Random.nextDouble(), Random.nextDouble(), Random.nextDouble())
      // scalastyle:off println
      println(s"The original should be ${CubeKeyFactory.old_createCubeKey(point, 100)}")
      var father = CubeKeyFactory.createCubeKey(point, 1)
      for (level <- 2.to(1000)) {
        val current = CubeKeyFactory.createCubeKey(point, level)
        current should startWith(father)
        father = current
      }

    }
  }

  "A 4D CubeKey" should {
    "always share the father prefix" in {

      val point =
        Point(Random.nextDouble(), Random.nextDouble(), Random.nextDouble(), Random.nextDouble())
      // scalastyle:off println
      println(s"The original should be ${CubeKeyFactory.old_createCubeKey(point, 100)}")
      var father = CubeKeyFactory.createCubeKey(point, 1)
      for (level <- 2.to(1000)) {
        val current = CubeKeyFactory.createCubeKey(point, level)
        current should startWith(father)
        father = current
      }

    }
  }

}
