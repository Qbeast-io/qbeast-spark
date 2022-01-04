/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

/**
 * Tests for CubeId.
 */
class CubeIdTest extends AnyFlatSpec with Matchers {
  "CubeId" should "implement equals correctly" in {
    val id1 = CubeId.root(2)
    val id2 = id1.firstChild
    val id3 = id1.firstChild
    val id4 = id2.nextSibling.get
    val id5 = CubeId.root(3)
    id1 == id1 shouldBe true
    id2 == id3 shouldBe true
    id1 == id2 shouldBe false
    id3 == id4 shouldBe false
    id1 == id5 shouldBe false
  }

  it should "implement conversion to byte array correctly" in {
    val point = Point(0.66, 0.83)
    for (id <- CubeId.containers(point).take(10)) {
      id shouldBe CubeId(2, id.bytes)
    }
  }

  it should "implement conversion to string correctly" in {
    val point = Point(0.66, 0.83, 0.2, 0.7, 0.4, 0.3, 0.1, 0.9)
    for (id <- CubeId.containers(point).take(20)) {
      id shouldBe CubeId(8, id.string)
      if (id.isRoot) {
        id.string shouldBe ""
      } else {
        val parentId = id.parent.get
        id.string should startWith(parentId.string)
      }
    }
  }

  it should "implement containers iterator correctly" in {
    val point = Point(0.66, 0.83)
    val List(id1, id2, id3, id4) = CubeId.containers(point).take(4).toList

    id1.isRoot shouldBe true
    id1.from shouldBe Point(0.0, 0.0)
    id1.to shouldBe Point(1.0, 1.0)

    id2.parent shouldBe Some(id1)
    id2.from shouldBe Point(0.5, 0.5)
    id2.to shouldBe Point(1.0, 1.0)

    id3.parent shouldBe Some(id2)
    id3.from shouldBe Point(0.5, 0.75)
    id3.to shouldBe Point(0.75, 1.0)

    id4.parent shouldBe Some(id3)
    id4.from shouldBe Point(0.625, 0.75)
    id4.to shouldBe Point(0.75, 0.875)
  }

  it should "implement containers iterator with parent correctly" in {
    val point = Point(0.66, 0.83)
    val List(_, id2, id3, id4) = CubeId.containers(point).take(4).toList
    val List(id5, id6) = CubeId.containers(point, id2).take(2).toList
    id5 shouldBe id3
    id6 shouldBe id4
  }

  it should "return correct container for 0" in {
    val point = Point(0.0, 0.0)
    val cubeId = CubeId.containers(point).drop(3).next()
    cubeId.from shouldBe Point(0.0, 0.0)
    cubeId.to shouldBe Point(0.125, 0.125)
  }

  it should "return correct container for 1" in {
    val point = Point(Vector(1.0, 1.0))
    val cubeId = CubeId.containers(point).drop(3).next()
    cubeId.from shouldBe Point(0.875, 0.875)
    cubeId.to shouldBe Point(1, 1)
  }

  it should "return correct container for border" in {
    val point = Point(Vector(1.0, 0.0))
    val cubeId = CubeId.containers(point).drop(3).next()
    cubeId.from shouldBe Point(0.875, 0.0)
    cubeId.to shouldBe Point(1.0, 0.125)
  }

  it should "implement parent correctly for root" in {
    CubeId.root(2).parent shouldBe None
  }

  it should "implement parent correctly for non root" in {
    val point = Point(0.66, 0.83)
    val parent = CubeId.containers(point).drop(3).next().parent.get
    parent.from shouldBe Point(0.5, 0.75)
    parent.to shouldBe Point(0.75, 1.0)
  }

  it should "implement firstChild correctly" in {
    val root = CubeId.root(2)
    val child = root.firstChild
    child.from shouldBe Point(0.0, 0.0)
    child.to shouldBe Point(0.5, 0.5)
  }

  it should "implement nextSibling correctly" in {
    val root = CubeId.root(2)
    val child = root.firstChild

    val sibling1 = child.nextSibling.get
    sibling1.from shouldBe Point(0.0, 0.5)
    sibling1.to shouldBe Point(0.5, 1.0)

    val sibling2 = sibling1.nextSibling.get
    sibling2.from shouldBe Point(0.5, 0.0)
    sibling2.to shouldBe Point(1.0, 0.5)

    val sibling3 = sibling2.nextSibling.get
    sibling3.from shouldBe Point(0.5, 0.5)
    sibling3.to shouldBe Point(1.0, 1.0)

    sibling3.nextSibling shouldBe None
  }

  it should "implement children iterator correctly" in {
    val List(id1, id2, id3, id4) = CubeId.root(2).children.toList
    id1.nextSibling shouldBe Some(id2)
    id2.nextSibling shouldBe Some(id3)
    id3.nextSibling shouldBe Some(id4)
    id4.nextSibling shouldBe None
  }

  it should "return a correct container with specified depth" in {
    val point = Point(0.66, 0.83)
    val id = CubeId.container(point, 2)
    id.from shouldBe Point(0.5, 0.75)
    id.to shouldBe Point(0.75, 1.0)
  }

  it should "implement contains correctly" in {
    val point = Point(0.66, 0.83)
    val cubeId = CubeId.container(point, 2)
    cubeId.contains(point) shouldBe true
    cubeId.contains(Point(0.0, 0.0)) shouldBe false
    cubeId.contains(Point(1.0, 1.0)) shouldBe false
    cubeId.contains(Point(0.5, 0.75)) shouldBe true
  }

  it should "compare parent and children correctly" in {

    val root = CubeId.root(2)
    val kids = root.children.toVector
    val grandChildren = Random.shuffle(kids.flatMap(_.children)).sorted
    for (kid <- kids) {
      root should be < kid
    }
    for (kid <- grandChildren) {
      root should be < kid
    }
    for (k <- kids) {
      for (kk <- k.children) {
        k should be < kk
      }
    }
    for (group <- grandChildren.grouped(4)) {
      group.map(_.parent).distinct.size shouldBe 1
    }
    //scalastyle:off
    grandChildren.foreach(a => println(a.string))

    val twoGens = Random.shuffle(kids ++ grandChildren).sorted
    println("TWO GENS BI2!")
    twoGens.foreach(a => println(a.string))
    for (group <- twoGens.grouped(5)) {
      group.takeRight(4).map(_.parent).distinct.size shouldBe 1
    }

  }
}
