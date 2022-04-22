import Dependencies._
import xerial.sbt.Sonatype._

lazy val qbeastCore = (project in file("core"))
  .settings(
    name := "qbeast-core",
    version := "0.1.0",
    libraryDependencies ++= Seq(apacheCommons % Test))

val qbeast_spark_version = "0.2.0"

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

qbeastSpark / Compile / doc / scalacOptions ++= Seq(
  "-doc-title",
  "qbeast-spark",
  "-doc-version",
  qbeast_spark_version,
  "-doc-footer",
  "Copyright 2022 Qbeast - Docs for version " + qbeast_spark_version + " of qbeast-spark")

// Common metadata
ThisBuild / version := qbeast_spark_version
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
  Compile / console / scalacOptions ~= {
    _.filterNot(
      Set("-Ywarn-unused-import", "-Ywarn-unused:imports", "-Xlint", "-Xfatal-warnings"))
  },
  Test / console / scalacOptions := (Compile / console / scalacOptions).value)

// Dependency repositories
ThisBuild / resolvers ++= Seq(Resolver.mavenLocal, Resolver.mavenCentral)

// Repository for maven (Tagged releases on Maven Central using Sonatype)
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) nexus + "content/repositories/snapshots"
  else nexus + "service/local"
}
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else sonatypePublishToBundle.value
}

// Sonatype settings
ThisBuild / publishMavenStyle := true
ThisBuild / sonatypeProfileName := "io.qbeast"
ThisBuild / sonatypeProjectHosting := Some(
  GitHubHosting(user = "Qbeast-io", repository = "qbeast-spark", email = "info@qbeast.io"))
ThisBuild / licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://qbeast.io/"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Qbeast-io/qbeast-spark"),
    "scm:git@github.com:Qbeast-io/qbeast-spark.git"))
ThisBuild / pomExtra :=
  <developers>
    <developer>
      <id>alexeiakimov</id>
      <name>Alexey Akimov</name>
      <url>https://github.com/alexeiakimov</url>
    </developer>
    <developer>
      <id>cugni</id>
      <name>Cesare Cugnasco</name>
      <url>https://github.com/cugni</url>
    </developer>
    <developer>
      <id>eavilaes</id>
      <name>Eric Avila</name>
      <url>https://github.com/eavilaes</url>
    </developer>
    <developer>
      <id>Jiaweihu08</id>
      <name>Jiawei Hu</name>
      <url>https://github.com/Jiaweihu08</url>
    </developer>
    <developer>
      <id>osopardo1</id>
      <name>Paola Pardo</name>
      <url>https://github.com/osopardo1</url>
    </developer>
    <developer>
      <id>polsm91</id>
      <name>Pol Santamaria</name>
      <url>https://github.com/polsm91</url>
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
