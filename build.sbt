import Dependencies._
import xerial.sbt.Sonatype._

lazy val qbeastCore = (project in file("core"))
  .settings(
    name := "qbeast-core",
    version := "0.1.0",
    commonSettings,
    releaseSettings,
    libraryDependencies ++= Seq(apacheCommons % Test))

lazy val qbeastSparkVersion = "0.2.0"

// Projects
lazy val qbeastSpark = (project in file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .dependsOn(qbeastCore)
  .settings(
    name := "qbeast-spark",
    version := qbeastSparkVersion,
    commonSettings,
    releaseSettings,
    libraryDependencies ++= Seq(
      qbeastCoreDep,
      deltaCore % Provided,
      sparkCore % Provided,
      sparkSql % Provided,
      hadoopClient % Provided,
      sparkFastTests % Test,
      amazonAws % Test,
      hadoopCommons % Test,
      hadoopAws % Test),
    noWarningInConsole,
    Compile / doc / scalacOptions ++= Seq(
      "-doc-title",
      "qbeast-spark",
      "-doc-version",
      qbeastSparkVersion,
      "-doc-footer",
      "Copyright 2022 Qbeast - Docs for version " + qbeastSparkVersion + " of qbeast-spark"),
    Test / parallelExecution := false,
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    publish / skip := true)

lazy val qbeastSparkNodep = (project in file("nodep"))
  .settings(
    name := "qbeast-spark-nodep",
    version := qbeastSparkVersion,
    commonSettings,
    publishGithubSettings,
    Compile / packageBin := (qbeastSpark / assembly).value)

// COMMON SETTINGS
lazy val commonSettings = Seq(
  organization := "io.qbeast",
  organizationName := "Qbeast Analytics, S.L.",
  organizationHomepage := Some(url("https://qbeast.io/")),
  startYear := Some(2021),
  libraryDependencies ++= Seq(fasterxml % Provided, scalaTest % Test, mockito % Test),
  Test / javaOptions += "-Xmx2G",
  Test / fork := true,
// Scala compiler settings
  scalaVersion := "2.12.12",
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8",
    "-Xfatal-warnings",
    "-feature",
    "-language:postfixOps",
    "-deprecation",
    "-unchecked",
    "-Xlint"),
// Java compiler settings
  javacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-source",
    "1.8",
    "-target",
    "1.8",
    "-Werror",
    "-g",
    "-Xlint",
    "-Xdoclint:all/package"))

// this setting remove warning when using the sbt console
lazy val noWarningInConsole = Seq(
  scalacOptions in (Compile, console) ~= {
    _.filterNot(
      Set("-Ywarn-unused-import", "-Ywarn-unused:imports", "-Xlint", "-Xfatal-warnings"))
  },
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value)

// Dependency repositories
ThisBuild / resolvers ++= Seq(Resolver.mavenLocal, Resolver.mavenCentral)

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

lazy val publishGithubSettings = Seq(
  // Publication repository (for nightly releases on GitHub)
  publishTo := Some("Qbeast Spark" at "https://maven.pkg.github.com/Qbeast-io/qbeast-spark"))

// RELEASE SETTINGS
lazy val releaseSettings = Seq(
  // Repository for maven (Tagged releases on Maven Central using Sonatype)
  sonatypeCredentialHost := "s01.oss.sonatype.org",
  sonatypeRepository := {
    val nexus = "https://s01.oss.sonatype.org/"
    if (isSnapshot.value) nexus + "content/repositories/snapshots"
    else nexus + "service/local"
  },
  publishTo := {
    val nexus = "https://s01.oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else sonatypePublishToBundle.value
  },
  // Sonatype settings
  publishMavenStyle := true,
  sonatypeProfileName := "io.qbeast",
  licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://qbeast.io/")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/Qbeast-io/qbeast-spark"),
      "scm:git@github.com:Qbeast-io/qbeast-spark.git")),
  pomExtra :=
    <developers>
      <developer>
        <id>Qbeast-io</id>
        <name>Qbeast-io</name>
        <url>https://github.com/Qbeast-io</url>
        <email>info@qbeast.io</email>
      </developer>
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
    </developers>)

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
