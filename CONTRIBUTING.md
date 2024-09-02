# Contribution Guide

Welcome to the Qbeast community! Nice to meet you :)

Here's a summary of what you can find in this page:

- [Introduction](#introduction)
- [Version control branching](#version-control-branching)
- [PRs and Issues](#prs-and-issues)
- [Style and formatting](#style-and-formatting)
- [Logging Documentation](#logging-documentation)
- [Developing and contributing](#developing-and-contributing)
  - [Development set up](#development-set-up)
  - [Developer documentation](#developer-documentation)
  - [Steps to contribute](#steps-to-contribute)
- [Publishing Guide](#publishing-guide)
- [Versioning](#versioning)
- [Licensing of contributed material](#licensing-of-contributed-material)
- [Community Values](#community-values)

# Introduction

Either you want to know more about our guidelines or open a Pull Request, this is your page. We are pleased to help you through the different steps for contributing to our (your) project.

To find Qbeast issues that make good entry points:

- Start with issues labelled [![good-first-issue](https://img.shields.io/github/labels/Qbeast-io/qbeast-spark/good%20first%20issue)](https://github.com/Qbeast-io/qbeast-spark/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). For example, see the good first issues in the repository for updates to the core Qbeast Spark code.

- For issues that require deeper knowledge of one or more technical aspects, look at issues labelled [![help wanted label](https://img.shields.io/github/labels/Qbeast-io/qbeast-spark/help%20wanted)](https://github.com/Qbeast-io/qbeast-spark/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22).

## Version control branching
- Always make a new branch for your work, no matter how small
- Don´t submit unrelated changes to the same branch/pull request
- Base your new branch off of the appropriate branch on the main repository
- New features should branch off of the `main` branch

## PRs and Issues

To open and merge PRs, the following is to be respected:

1. **Always open an issue for the PR you're working** with as much detail as possible.
2. Every PR should have an issue that it is trying to address, ideally one.
3. The **title of the PR** should follow the schema: Issue &lt;issue-number&gt;: &lt;PR-title&gt;
4. Ideally, there should be **at least two reviewers per PR**
5. The author of the PR never gets to merge the PR; **a PR can only be merged by a reviewer**.
6. Do a **Squash and Merge instead of merge**, again, by a reviewer.
7. Make sure the commit messages in the Squash Merge are clear and concise and reflect all major changes introduced by the PR.

## Style and formatting
- We follow [Scalastyle](http://www.scalastyle.org) for coding style in Scala. It runs at compile time, but you can check it manually with: 

  ```bash
  sbt scalastyle
  ```

- [Scalafmt](https://scalameta.org/scalafmt/) is used for code formatting. You can configure your IDE to [reformat at save](https://scalameta.org/scalafmt/docs/installation.html#format-on-save):

    ![Format on Save on IntelliJ](https://scalameta.org/scalafmt/docs/assets/img/intellij-on-save-native.png)

  Or alternatively force code formatting:

  ```bash 
  sbt scalafmt # Format main sources

  sbt test:scalafmt # Format test sources
 
  sbt scalafmtCheck # Check if the scala sources under the project have been formatted

  sbt scalafmtSbt # Format *.sbt and project/*.scala files

  sbt scalafmtSbtCheck # Check if the files have been formatted by scalafmtSbt
  ```

- Sbt also checks the format of the Scala docs when publishing the artifacts. The following command will check and generate the [Scaladocs](https://docs.scala-lang.org/style/scaladoc.html):

  ```bash
  sbt doc
  ```
  
## Logging Documentation

- We use the [Spark Logging interface](https://spark.apache.org/docs/1.5.2/api/java/org/apache/spark/Logging.html#:~:text=Interface%20Logging&text=Creates%20a%20SLF4J%20logger%20for,intended%20as%20an%20internal%20utility).
- Spark uses `log4j` for logging. You can configure it by adding a `log4j2.properties` file in the conf directory. One way to start is to copy the existing `log4j2.properties.template` located there.

An example of using logging on a class is:

```scala
import org.apache.spark.internal.Logging

case class MyClass() extends Logging {

  def myMethod(): Unit = {
    logInfo("This is an info message")
    logWarn("This is a warning message")
    logError("This is an error message")
    logTrace("This is a trace message")
    logDebug("This is a debug message")
  }
}

```

The following log levels are used to track code behaviour:
- `WARN` level is supposed to be critical and actionable. If the user sees a WARN, then something bad happened and it might require user intervention. 
  
  **Example on `DeltaMetadataWriter` class:**
   ```scala
     def writeWithTransaction(writer: => (TableChanges, Seq[FileAction])): Unit = {
      // [...] Code to write the transaction [...]
      if (txn.appId == appId && version <= txn.version) {
        val message = s"Transaction ${version} from application ${appId} is already completed," +
          " the requested write is ignored"
        logWarn(message)
        return
      }
    }
   ```
- `INFO` level provides information about the execution, but not necessarily actionable and it avoids being verbose. It is not uncommon to see INFO level on in production, so it is expected to be lightweight with respect to the volume of messages generated. 
  
  **Example on `BlockWriter` class:**
   ```scala
  def writeRow(rows: Iterator[InternalRow]): Iterator[(AddFile, TaskStats)] = {
     // [...] Code to write the rows [...]
     logInfo(s"Adding file ${file.path}") 
  }
   ```
- `DEBUG` provides debug level info when debugging the code. It can be verbose as it is not expected to be on in production.
  
  **Example on `IndexedTable` class:**
   ```scala
  if (isNewRevision(options)) {
    // Merging revisions code
    logDebug(
      s"Merging transformations for table ${tableID} with cubeSize=${newRevisionCubeSize}")
    // Code to merge revisions
   }
   ```
- `TRACE` provides further detail to DEBUG on execution paths, and in particular, it indicates the execution of critical methods.
  
  **Example on `IndexedTable` class:**
     ```scala
     def doWrite(
        data: DataFrame,
        indexStatus: IndexStatus,
        options: QbeastOptions,
        append: Boolean): Unit = {
      logTrace(s"Begin: Writing data to table ${tableID}")
      // [...] Code to write the data [...]
      logTrace(s"End: Writing data to table ${tableID}")
     }
     ```

> We should enforce **all the Pull Request**, specially those containing critical code, **to have logging messages** that are meaningful and informative.

# Developing and contributing

## Development set up

### 1. Install [**sbt**(>=1.4.7)](https://www.scala-sbt.org/download.html).

### 2. Install **Spark**
Download **Spark 3.3.0 with Hadoop 3.3***, unzip it, and create the `SPARK_HOME` environment variable:<br />
*: You can use Hadoop 2.7 or 3.2 if desired, but you could have some troubles with different cloud providers' storage, read more about it [here](docs/CloudStorages.md).

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

tar -xzvf spark-3.5.0-bin-hadoop3.tgz

export SPARK_HOME=$PWD/spark-3.5.0-bin-hadoop3
 ```

### 3. Project packaging:
Navigate to the repository folder and package the project using **sbt**. [JDK 8](https://www.azul.com/downloads/?version=java-8-lts&package=jdk) is recommended.

> ℹ️ **Note**: You can specify **custom** Spark or Hadoop **versions** when packaging by using  
>`-Dspark.version=3.5.0` or `-Dhadoop.version=2.7.4` when running `sbt assembly`.
If you have troubles with the versions you use, don't hesitate to **ask the community** in [GitHub discussions](https://github.com/Qbeast-io/qbeast-spark/discussions).

``` bash
cd qbeast-spark

sbt assembly
```
This code generates a fat jar with all required dependencies (or most of them) shaded.

The jar does not include scala nor spark nor delta, and it is supposed to be used inside spark. 

For example: 
```bash
sbt assembly

$SPARK_HOME/bin/spark-shell \
--jars ./target/scala-2.12/qbeast-spark-assembly-0.6.0.jar \
--packages io.delta:delta-spark_2.12:3.1.0 \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
```

### 4. Publishing artefacts in the local repository
Sometimes it is convenient to have custom versions of the library to be
published in the local repository like IVy or Maven. For local Ivy (`~/.ivy2`)
use

```bash
sbt publishLocal
```

For local Maven (~/.m2) use

```bash
sbt publishM2
```

<br/>

## Developer documentation
You can find the developer documentation (Scala docs) in the [https://docs.qbeast.io/](https://docs.qbeast.io/).

## Steps to contribute
#### 1 - Click `Fork` on Github, and name it as `yourname/projectname`

#### 2 - Clone the project: `git clone git@github.com:yourname/projectname`

#### 3 - Create a branch: `git checkout -b your-branch-name`

#### 4 - Open the project with your IDE, and install extensions for Scala and sbt

#### 5 - Make your changes

#### 6 - Write, run, and pass tests:
> **Note**: You can run tests for different Spark and Hadoop versions specifying them with `-Dspark.version` and `-Dhadoop.version`.
Find an example in the _Setting Up_ section of [README.md](README.md)
- For all tests:

  ```bash
  sbt test
  ```

- To run a particular test only:

  ```bash
  sbt "testOnly io.qbeast.spark.index.writer.BlockWriterTest"
  ```

#### 7 - Commit your changes: `git commit -m "mychanges"`

#### 8 - Push your commit to get it back up to your fork: `git push origin HEAD`

#### 9 - Go to Github and open a [![pull request](https://img.shields.io/badge/-pull--request-yellow)](https://github.com/Qbeast-io/qbeast-spark/compare) against the `main` branch of qbeast-spark.
Identify the committers and contributors who have worked on the code being changed, and ask for their review :+1:

#### 10 - Wait for the maintainers to get back to you as soon as they can!

# Publishing Guide

To publish a new version of the qbeast-spark project, follow these steps:

> ℹ️ **Note**: This example is for a new version 0.6.0. The same steps can be applied to any new version.

1. Update the version in the `build.sbt` file. 
    ```scala
    version := "0.6.0-SNAPSHOT"
    ```
2. Check that all tests passed. 
    ```bash
    sbt test
    ```
3. Package the code with `sbt package` and run the code in the `README` for a quick test on spark-shell. 
    ```bash
    sbt package
    ```
4. Publish a `SNAPSHOT`.
   ```bash
   sbt publishSigned
   ```

5. Try the same `README` code downloading the `SNAPSHOT` with `--packages` and `--repositories` configuration. If necessary, test it in different Cloud Provider environments.
```bash
export QBEAST_SPARK_VERSION=0.6.0-SNAPSHOT
$SPARK_350/bin/spark-shell --repositories https://s01.oss.sonatype.org/content/repositories/snapshots \
--packages io.delta:delta-spark_2.12:3.1.0,io.qbeast:qbeast-spark_2.12:$QBEAST_SPARK_VERSION \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog

```
6. If everything is ok, change the `build.sbt` with the corresponding version and publish the RC.
    ```scala
    version := "0.6.0-rc1"
    ```
    ```bash
    sbt clean
    # Separate commands for publishing
    sbt publishSigned
    sbt sonatypePrepare
    sbt sonatypeBundleUpload
    sbt sonatypeRelease
    ```
    Or use one single command:
    ```bash
    sbt clean
    sbt sonatypeBundleRelease
    ```
   
7. Tag the code and push it to Github to make it public.
    ```bash
    git tag -a v0.6.0-rc1 -m "Release Candidate 1"
    git push origin v0.6.0-rc1
    ```
8. Set up the RC for approval. Make sure all users that had tested the version in their dev environments didn't experience any bug or performance drawback.

9. If the RC is approved, go to step 1, change the version in the `build.sbt` file and publish the final version.


Read all about **How to Publish an SBT Project into Maven Central** [here](https://qbeast.io/publish-your-sbt-project-to-the-central-repository/).
# Versioning

Here are some rules on versioning the qbeast-spark project that must be applied before a new version is released.

## post 0.6.0 release

If the current version is 0.y.z, these rules must be applied for a new release:
- 0.y.z+1 for a bugfix only release
- 0.z+1.0 for a new feature(s) release
- 0.y.z-rcN for release candidates, N being a positive integer

## post 1.0.0 release

We'll apply Semantic Versioning rules as defined at [semver.org](https://semver.org).

## Snapshots

Snapshots can be made available for internal and test purposes.
In this case the versioning to be applied is: `<new-version-number>-<short-commit-sha>-SNAPSHOT`

Example: `0.6.2-badfbadf-SNAPSHOT`


# Licensing of contributed material
All contributed code, docs, and other materials are considered licensed under the same terms as the rest of the project. Check [LICENSE](./LICENSE) for more details.


# Community Values

The following are the refined values that our community has developed in order to promote continuous improvement of our projects and peers.



## Make your life easier

Automate. Large projects are hard work. We value time spent automating repetitive work. Where that work cannot be automated, it is our culture to recognize and reward all types of contributions.



## We're all in this together

Innovation requires alternate points of view and ranges of abilities which must be heard in an inviting and respectful environment. Community leadership is acquired through hard work, quality, quantity and commitment. Our community recognizes the time and effort put into a discussion, no matter where a contributor is on their growth journey.



## Always Evolve

Qbeast Spark is a stronger project because it is open to fresh ideas. The Qbeast project culture is built on the principles of constant improvement, servant leadership, mentorship, and respect. Leaders in the Qbeast community are responsible for finding, sponsoring, and promoting new members. Leaders should expect to step aside. Members in the community should be prepared to step up.



## Community comes first

We're here first and foremost as a community, and our devotion is to the Qbeast project's intentional care for the benefit of all its members and users globally. We believe in publicly cooperating toward the common objective of creating a dynamic, interoperable ecosystem ensuring our users have the best experience possible.

Individuals achieve status by their efforts, and businesses gain status via their commitments to support this community and fund the resources required to keep the project running.



## Distributing is Caring

The scale of the Qbeast Spark project can only be achieved through highly reliable and highly visible assignments, including authorization, decision-making, technical design, code ownership, and documentation. Our global community is built on distributed asynchronous ownership, cooperation, communication, and decision-making.


As Pablo Picasso once said:

> **"Action is the foundational key to all success."**