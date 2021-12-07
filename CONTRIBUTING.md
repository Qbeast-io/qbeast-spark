# Contribution Guide

Welcome to the Qbeast community! Nice to meet you :)

Either you want to know more about our guidelines or open a Pull Request, this is your page. We are pleased to help you through the different steps for contributing to our (your) project. 

## Licensing of contributed material
All contributed code, docs, and other materials are considered licensed under the same terms as the rest of the project. Check [LICENSE](./LICENCE) for more details.

## Version control branching
- Always make a new branch for your work, no matter how small
- Don´t submit unrelated changes to the same branch/pull request
- Base your new branch off of the appropriate branch on the main repository
- New features should branch off of the `master` branch

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

## Step-by-step guide
  1 - Click `Fork` on Github, and name it as `yourname/projectname`

  2 - Clone the project: `git clone git@github.com:yourname/projectname`

  3 - Create a branch: `git checkout -b your-branch-name`

  4 - Open the project with your IDE, and install extensions for Scala and sbt

  5 - Make your changes

  6 - Write, run, and pass tests:  
**Note**: You can run tests for different Spark and Hadoop versions specifying them with `-Dspark.version` and `-Dhadoop.version`.
  Find an example in the _Setting Up_ section of [README.md](README.md)
  - For all tests:
    
  ```bash
  sbt test
  ```
    
  - To run a particular test only:
  
  ```bash
  sbt "testOnly io.qbeast.spark.index.writer.BlockWriterTest"
  ```

  7 - Commit your changes: `git commit -m "mychanges"`

  8 - Push your commit to get it back up to your fork: `git push origin HEAD`

  9 - Go to Github and open a `pull request` against the `master` branch of qbeast-spark. Identify the committers and contributors who have worked on the code being changed

  10 - Wait for the maintainers to get back to you as soon as they can!

## How to use the code
This code generates a fat jar with all required dependencies (or most of them) shaded.

The jar does not include scala nor spark nor delta, and it is supposed to be used inside spark. 

For example: 
```bash
sbt assembly

$SPARK_HOME/bin/spark-shell \
--jars ./target/scala-2.12/qbeast-spark-assembly-0.2.0.jar \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--packages io.delta:delta-core_2.12:1.0.0
```

### Overriding configurations

There are some default configuration for the qbeast index parameters:

- `index.size` determines the number of elements that could fit into each cube

These parameters can be modified as below when starting the spark shell:

  ```bash
  --driver-java-options "-Dqbeast.index.size=28927386"
  ```

<br/>

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