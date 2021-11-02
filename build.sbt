import Dependencies._

lazy val qbeastCore = (project in file("core")).settings(
  name := "qbeast-core",
  libraryDependencies ++= Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.0",
    typesafeConf,
    sparkFastTests % Test,
    scalaTest % Test,
    mockito % Test,
    apacheCommons % Test))

// Projects
lazy val qbeastSpark = (project in file("."))
  .dependsOn(qbeastCore)
  .settings(
    name := "qbeast-spark",
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.0",
      sparkCore % Provided,
      sparkSql % Provided,
      hadoopClient % Provided,
      deltaCore % Provided,
      typesafeConf,
      sparkFastTests % Test,
      scalaTest % Test,
      mockito % Test),
    Test / parallelExecution := false,
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    assembly / assemblyShadeRules := shadingRules,
    publish / skip := true)
  .settings(noWarningInConsole)

lazy val qbeastSparkNodep = (project in file("nodep"))
  .settings(name := "qbeast-spark-nodep", Compile / packageBin := (qbeastSpark / assembly).value)

// Common metadata
ThisBuild / version := "0.1.0"
ThisBuild / organization := "io.qbeast"
ThisBuild / organizationName := "Qbeast Analytics, S.L."
ThisBuild / organizationHomepage := Some(url("https://qbeast.io/"))
ThisBuild / startYear := Some(2021)

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

// Publication repository
ThisBuild / publishTo := Some(
  "Qbeast Spark" at "https://maven.pkg.github.com/Qbeast-io/qbeast-spark")

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

lazy val shadingRules =
  Seq("com.typesafe")
    .map(pack => ShadeRule.rename(f"$pack.**" -> f"io.qbeast.spark.shaded.$pack.@1").inAll)
