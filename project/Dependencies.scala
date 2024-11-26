import sbt._

/**
 * External libraries used in the project with versions.
 */
object Dependencies {
  lazy val sparkVersion: String = sys.props.get("spark.version").getOrElse("3.5.2")
  lazy val hadoopVersion: String = sys.props.get("hadoop.version").getOrElse("3.3.4")
  lazy val deltaVersion: String = "3.1.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % hadoopVersion
  val deltaSpark = "io.delta" %% "delta-spark" % deltaVersion
  val sparkFastTests = "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
  val mockito = "org.scalatestplus" %% "mockito-3-4" % "3.2.9.0"
  val apacheCommons = "org.apache.commons" % "commons-lang3" % "3.10"
  val amazonAws = "com.amazonaws" % "aws-java-sdk" % "1.12.20"
  val hadoopCommons = "org.apache.hadoop" % "hadoop-common" % hadoopVersion
  val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % hadoopVersion
  val fasterxml = "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.0"
  val sparkml = "org.apache.spark" %% "spark-mllib" % sparkVersion
}
