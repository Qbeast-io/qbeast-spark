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
package io.qbeast.spark.utils

import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Random

class QbeastCreateTableSQLTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createStudentsTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "Qbeast SQL" should "create EXTERNAL existing indexedTable WITHOUT options" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val location = tmpDir + "/external_student/"
      val data = createStudentsTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id,name").save(location)

      spark.sql(
        "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
          "USING qbeast " +
          s"LOCATION '$location'")

    })

  it should "throw an error if the indexedTable is NOT qbeast" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val location = tmpDir + "/external_student/"
      val data = createStudentsTestData(spark)
      data.write.format("delta").save(location)

      an[AnalysisException] shouldBe thrownBy(
        spark.sql(
          "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
            "USING qbeast " +
            s"LOCATION '$location'"))

      an[AnalysisException] shouldBe thrownBy(
        spark.sql(
          "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
            "USING qbeast " +
            "OPTIONS ('columnsToIndex'='id') " +
            s"LOCATION '$location'"))

    })

  it should "NOT overwrite existing columnsToIndex if specified" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      // WRITE INITIAL DATA WITH QBEAST
      val location = tmpDir + "/external_student/"
      val data = createStudentsTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(location)

      // COLUMNS TO INDEX ARE CHANGED
      spark.sql(
        "CREATE EXTERNAL TABLE student_column_change (id INT, name STRING, age INT) " +
          "USING qbeast " +
          "OPTIONS ('columnsToIndex'='id,name') " +
          s"LOCATION '$location'")

      // COLUMNS TO INDEX CANNOT BE CHANGED
      an[AnalysisException] shouldBe thrownBy(data.writeTo("student_column_change").append())
    })

  it should "overwrite existing CUBE SIZE options if specified" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      // WRITE INITIAL DATA WITH QBEAST
      val location = tmpDir + "/external_student/"
      val data = createStudentsTestData(spark)
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("cubeSize", "100")
        .save(location)

      spark.sql(
        "CREATE EXTERNAL TABLE student_cube_change (id INT, name STRING, age INT) " +
          "USING qbeast " +
          "OPTIONS ('cubeSize'='50') " +
          s"LOCATION '$location'")

      val qbeastTable = QbeastTable.forPath(spark, location)
      qbeastTable.cubeSize() shouldBe 100

      data.writeTo("student_cube_change").append()

      spark.sql("SELECT * FROM student_cube_change").count() shouldBe data.count() * 2
      qbeastTable.cubeSize() shouldBe 50

    })

  it should "create indexedTable even if location is not populated" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val location = tmpDir + "/external_student/"

      spark.sql(
        "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
          "USING qbeast " +
          "OPTIONS ('columnsToIndex'='id,name') " +
          s"LOCATION '$location'")

      // SELECT FROM
      spark.sql("SELECT * FROM student").count() shouldBe 0

      // WRITE TO
      val data = createStudentsTestData(spark)
      data.writeTo("student").append()

      // SELECT FROM
      spark.read.format("qbeast").load(location).count() shouldBe data.count()
      spark.sql("SELECT * FROM student").count() shouldBe data.count()
      val qbeastTable = QbeastTable.forPath(spark, location)
      qbeastTable.indexedColumns() shouldBe Seq("id", "name")

    })

}
