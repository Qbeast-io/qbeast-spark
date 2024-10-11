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
package io.qbeast.spark.delta

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.qbeast.core.model.mapper
import io.qbeast.core.model.QbeastStats
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeltaQbeastStatsTest extends AnyFlatSpec with Matchers {

  mapper.registerModule(QbeastStatsUtils.module)

  def areJsonEqual(json1: String, json2: String): Boolean = {
    val basicMapper = new ObjectMapper()

    try {
      val node1: JsonNode = basicMapper.readTree(json1)
      val node2: JsonNode = basicMapper.readTree(json2)

      val normalizedJson1 =
        basicMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node1)
      val normalizedJson2 =
        basicMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node2)

      // Compare the normalized JSON strings
      normalizedJson1 == normalizedJson2
    } catch {
      case e: Exception =>
        // Handle any parsing errors
        println(s"Error parsing JSON: ${e.getMessage}")
        false
    }
  }

  "ValueSerializer" should "serialize a numeric string to a number" in {
    val json = mapper.writeValueAsString("123") // This uses ValueSerializer
    json shouldEqual "123" // It should output a number in JSON
  }

  it should "serialize a non-numeric string as is" in {
    val json = mapper.writeValueAsString("hello") // This uses ValueSerializer
    json shouldEqual "\"hello\""
  }

  "ValueDeserializer" should "deserialize a number string to a string" in {
    val value = mapper.readValue("123", classOf[String]) // This uses ValueDeserializer
    value shouldEqual "123"
  }

  it should "deserialize a textual value as is" in {
    val value = mapper.readValue("\"hello\"", classOf[String]) // This uses ValueDeserializer
    value shouldEqual "hello"
  }

  "MapDeserializer" should "deserialize a JSON object to a Map[String, String]" in {
    val json = """{"key1": "value1", "key2": {"innerKey": "innerValue"}}"""
    val result = mapper.readValue(json, classOf[Map[String, String]]) // This uses MapDeserializer

    result should contain("key1" -> "value1")
    result should contain("key2" -> """{"innerKey":"innerValue"}""")
  }

  "QbeastStatsUtils" should "serialize and deserialize correctly" in {
    val jsonString =
      """{"numRecords":52041,"minValues":{"key1": "value1", "key2": {"innerKey": "innerValue"}},
        |"maxValues":{"key3": "value3", "key4": "value4"},"nullCount":{"key5": 0, "key6": 2}}""".stripMargin

    // Create the expected QbeastStats object
    val expectedStats = QbeastStats(
      numRecords = 52041,
      minValues = Map("key2" -> """{"innerKey":"innerValue"}""", "key1" -> "value1"),
      maxValues = Map("key3" -> "value3", "key4" -> "value4"),
      nullCount = Map("key5" -> "0", "key6" -> "2"))

    // Deserialize the JSON string
    val deserializedStats = QbeastStatsUtils.fromString(jsonString)

    // Verify that deserialization was successful and matches expected values
    deserializedStats shouldBe defined // Ensure deserialization was successful

    deserializedStats.foreach { ds =>
      ds.numRecords shouldEqual expectedStats.numRecords
      ds.minValues should contain theSameElementsAs expectedStats.minValues
      ds.maxValues should contain theSameElementsAs expectedStats.maxValues
      ds.nullCount should contain theSameElementsAs expectedStats.nullCount
    }

    // Serialize back to JSON string and verify it matches the original
    val serializedJsonString = QbeastStatsUtils.toString(deserializedStats.get)
    areJsonEqual(serializedJsonString, jsonString)
  }

}
