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

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.qbeast.config
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastConfigTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "Qbeast config" should "use default configurations" in withSpark { _ =>
    config.DEFAULT_CUBE_SIZE shouldBe 5000000
    config.CUBE_DOMAINS_BUFFER_CAPACITY shouldBe 100000L
    config.DEFAULT_NUMBER_OF_RETRIES shouldBe 2
    config.COLUMN_SELECTOR_ENABLED shouldBe false
    config.MAX_NUM_COLUMNS_TO_INDEX shouldBe 3
    config.DEFAULT_TABLE_FORMAT shouldBe "delta"
  }

  it should "change configurations accordingly" in withExtendedSpark(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.defaultCubeSize", "1000")
      .set("spark.qbeast.index.cubeDomainsBufferCapacity", "2000")
      .set("spark.qbeast.index.numberOfRetries", "5")
      .set("spark.qbeast.index.columnsToIndex.auto", "true")
      .set("spark.qbeast.index.columnsToIndex.auto.max", "10")
      .set("spark.qbeast.tableFormat", "delta")) { _ =>
    config.DEFAULT_CUBE_SIZE shouldBe 1000
    config.CUBE_DOMAINS_BUFFER_CAPACITY shouldBe 2000L
    config.DEFAULT_NUMBER_OF_RETRIES shouldBe 5
    config.COLUMN_SELECTOR_ENABLED shouldBe true
    config.MAX_NUM_COLUMNS_TO_INDEX shouldBe 10
    config.DEFAULT_TABLE_FORMAT shouldBe "delta"
  }

  it should "handle missing or default values for unset configurations" in withSpark { _ =>
    SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.index.defaultCubeSize") shouldBe None
    SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.index.cubeDomainsBufferCapacity") shouldBe None
    SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.index.numberOfRetries") shouldBe None
    SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.index.columnsToIndex.auto") shouldBe None
    SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.index.columnsToIndex.auto.max") shouldBe None
    SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.tableFormat") shouldBe None
  }

  it should "handle invalid input gracefully for all configurations" in withExtendedSpark(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.defaultCubeSize", "invalid")
      .set("spark.qbeast.index.cubeDomainsBufferCapacity", "invalid")
      .set("spark.qbeast.index.numberOfRetries", "invalid")
      .set("spark.qbeast.index.columnsToIndex.auto", "invalid")
      .set("spark.qbeast.index.columnsToIndex.auto.max", "invalid")
      .set("spark.qbeast.tableFormat", "invalid")) { _ =>
    intercept[IllegalArgumentException] {
      config.DEFAULT_CUBE_SIZE
    }
    intercept[IllegalArgumentException] {
      config.CUBE_DOMAINS_BUFFER_CAPACITY
    }
    intercept[IllegalArgumentException] {
      config.DEFAULT_NUMBER_OF_RETRIES
    }
    intercept[IllegalArgumentException] {
      config.COLUMN_SELECTOR_ENABLED
    }
    intercept[IllegalArgumentException] {
      config.MAX_NUM_COLUMNS_TO_INDEX
    }
    intercept[IllegalArgumentException] {
      config.DEFAULT_TABLE_FORMAT
    }
  }

}
