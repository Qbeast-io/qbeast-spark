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
package io.qbeast.context

import io.qbeast.core.QbeastCoreTestSpec
import org.apache.spark.qbeast
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastConfigTest extends AnyFlatSpec with Matchers with QbeastCoreTestSpec {

  "Qbeast config" should "use default configurations" in withSpark { _ =>
    config.DEFAULT_CUBE_SIZE shouldBe 5000000
    config.CUBE_WEIGHTS_BUFFER_CAPACITY shouldBe 100000
  }

  it should "change configurations accordingly" in withExtendedSpark(
    sparkConf
      .set("spark.qbeast.index.defaultCubeSize", "1000")
      .set("spark.qbeast.index.cubeWeightsBufferCapacity", "1000")
      .set("spark.qbeast.index.numberOfRetries", "10")) { _ =>
    config.DEFAULT_CUBE_SIZE shouldBe 1000
    config.CUBE_WEIGHTS_BUFFER_CAPACITY shouldBe 1000
    config.DEFAULT_NUMBER_OF_RETRIES shouldBe 10

  }

  "Spark.qbeast.keeper" should "not be defined" in withSpark { _ =>
    val keeperString = SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.keeper")
    keeperString shouldBe None
  }

  it should "be defined" in withExtendedSpark(
    sparkConf
      .set("spark.qbeast.keeper", "myKeeper")) { spark =>
    val keeperString = spark.sparkContext.getConf.getOption("spark.qbeast.keeper")
    keeperString.get shouldBe "myKeeper"
  }

}
