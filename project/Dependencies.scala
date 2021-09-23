import sbt._

/**
 * External libraries used in the project with versions.
 */
object Dependencies {
  lazy val sparkVersion = "3.1.1"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val deltaCore = "io.delta" %% "delta-core" % "0.8.0"
  val typesafeConf = "com.typesafe" % "config" % "1.2.0"
  val sparkFastTests = "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
  val mockito = "org.scalatestplus" %% "mockito-3-4" % "3.2.9.0"
}
