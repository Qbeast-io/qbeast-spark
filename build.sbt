import Dependencies._
import xerial.sbt.Sonatype._

lazy val qbeastCore = (project in file("core"))
  .settings(name := "qbeast-core", libraryDependencies ++= Seq(apacheCommons % Test))

// Projects
lazy val qbeastSpark = (project in file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .dependsOn(qbeastCore)
  .settings(
    name := "qbeast-spark",
    libraryDependencies ++= Seq(
      sparkCore % Provided,
      sparkSql % Provided,
      hadoopClient % Provided,
      deltaCore % Provided,
      amazonAws % Test,
      hadoopCommons % Test,
      hadoopAws % Test),
    Test / parallelExecution := false,
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    publish / skip := true)
  .settings(noWarningInConsole)

lazy val qbeastSparkNodep = (project in file("nodep"))
  .settings(name := "qbeast-spark-nodep", Compile / packageBin := (qbeastSpark / assembly).value)

// As root project has publish / skip := true, we need to create a wrapper project to publish on
// sonatype and Maven Central, which has "qbeast-spark" as name.
lazy val qbeastSparkMaven = (project in file("maven"))
  .settings(name := "qbeast-spark", Compile / packageBin := (qbeastSpark / assembly).value)

// Common metadata
ThisBuild / version := "0.2.0"
ThisBuild / organization := "io.qbeast"
ThisBuild / organizationName := "Qbeast Analytics, S.L."
ThisBuild / organizationHomepage := Some(url("https://qbeast.io/"))
ThisBuild / startYear := Some(2021)
ThisBuild / libraryDependencies ++= Seq(
  fasterxml % Provided,
  sparkFastTests % Test,
  scalaTest % Test,
  mockito % Test)

Test / javaOptions += "-Xmx2G"
Test / fork := true

// Scala compiler settings
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-target:jvm-1.8",
  "-Xfatal-warnings",
  "-feature",
  "-language:postfixOps",
  "-deprecation",
  "-unchecked",
  "-Xlint")

// Java compiler settings
ThisBuild / javacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-source",
  "1.8",
  "-target",
  "1.8",
  "-Werror",
  "-g",
  "-Xlint",
  "-Xdoclint:all/package")

// this setting remove warning when using the sbt console
lazy val noWarningInConsole = Seq(
  scalacOptions in (Compile, console) ~= {
    _.filterNot(
      Set("-Ywarn-unused-import", "-Ywarn-unused:imports", "-Xlint", "-Xfatal-warnings"))
  },
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value)

// Dependency repositories
ThisBuild / resolvers ++= Seq(Resolver.mavenLocal, Resolver.mavenCentral)

// Publication repository (for nightly releases on GitHub)
ThisBuild / publishTo := Some(
  "Qbeast Spark" at "https://maven.pkg.github.com/Qbeast-io/qbeast-spark")

// Repository for maven (Tagged releases on Maven Central using Sonatype)
qbeastSparkMaven / publishTo := sonatypePublishToBundle.value
qbeastSparkMaven / sonatypeCredentialHost := "s01.oss.sonatype.org"

// GitHub Package Registry credentials
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse(
    // Any user will work if you're using a TOKEN as password
    "GHPR_USERNAME",
    "GHPR_USERNAME required to fetch or publish from/to GH Package Registry"),
  sys.env.getOrElse(
    "GHPR_TOKEN",
    "GHPR_TOKEN required to fetch or publish from/to GitHub Package Registry"))

// Sonatype settings
qbeastSparkMaven / publishMavenStyle := true
qbeastSparkMaven / sonatypeProfileName := "io.qbeast"
qbeastSparkMaven / sonatypeProjectHosting := Some(
  GitHubHosting(user = "Qbeast-io", repository = "qbeast-spark", email = "info@qbeast.io"))
qbeastSparkMaven / licenses := Seq(
  "APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
qbeastSparkMaven / homepage := Some(url("https://qbeast.io/"))
qbeastSparkMaven / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Qbeast-io/qbeast-spark"),
    "scm:git@github.com:Qbeast-io/qbeast-spark.git"))
qbeastSparkMaven / pomExtra :=
  <developers>
    <developer>
      <id>osopardo1</id>
      <name>Paola Pardo</name>
      <url>https://github.com/osopardo1</url>
    </developer>
    <developer>
      <id>eavilaes</id>
      <name>Eric Avila</name>
      <url>https://github.com/eavilaes</url>
    </developer>
    <developer>
      <id>cugni</id>
      <name>Cesare Cugnasco</name>
      <url>https://github.com/cugni</url>
    </developer>
    <developer>
      <id>Jiaweihu08</id>
      <name>Jiawei Hu</name>
      <url>https://github.com/Jiaweihu08</url>
    </developer>
    <developer>
      <id>alexeiakimov</id>
      <name>Alexey Akimov</name>
      <url>https://github.com/alexeiakimov</url>
    </developer>
  </developers>

// Scalafmt settings
Compile / compile := (Compile / compile).dependsOn(Compile / scalafmtCheck).value
Test / compile := (Test / compile).dependsOn(Test / scalafmtCheck).value

// Scalastyle settings
Compile / compile := (Compile / compile).dependsOn((Compile / scalastyle).toTask("")).value
Test / compile := (Test / compile).dependsOn((Test / scalastyle).toTask("")).value

// Header settings
headerLicense := Some(HeaderLicense.Custom("Copyright 2021 Qbeast Analytics, S.L."))
headerEmptyLine := false
Compile / compile := (Compile / compile).dependsOn(Compile / headerCheck).value
