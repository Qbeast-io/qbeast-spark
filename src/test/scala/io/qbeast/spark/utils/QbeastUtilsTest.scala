package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException

class QbeastUtilsTest extends QbeastIntegrationTestSpec {

  "QbeastUtils" should "compute histogram for string columns" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a", "b", "c", "a", "b", "c", "a", "b", "c").toDF("name")
      val hist = QbeastUtils.computeHistogramForColumn(df, "name")

      hist shouldBe "['a','b','c']"
    })

  it should "compute histogram for Int" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df = Seq(1, 2, 3, 1, 2, 3, 1, 2, 3).toDF("age")
    val hist = QbeastUtils.computeHistogramForColumn(df, "age")

    hist shouldBe "['1','2','3']"
  })

  it should "throw error when the column does not exists" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a").toDF("name")
      an[AnalysisException] shouldBe thrownBy(
        QbeastUtils.computeHistogramForColumn(df, "non_existing_column"))
    })

}
