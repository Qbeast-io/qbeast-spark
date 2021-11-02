import sbt._

/**
 * External libraries used in the project with versions.
 */
object Dependencies {
  lazy val sparkVersion: String = sys.props.get("spark.version").getOrElse("3.1.2")
  lazy val hadoopVersion: String = sys.props.get("hadoop.version").getOrElse("3.2.0")

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % hadoopVersion
  val deltaCore = "io.delta" %% "delta-core" % "1.0.0"
  val typesafeConf = "com.typesafe" % "config" % "1.2.0"
  val sparkFastTests = "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
  val mockito = "org.scalatestplus" %% "mockito-3-4" % "3.2.9.0"
  val apacheCommons = "org.apache.commons" % "commons-lang3" % "3.10"
}
