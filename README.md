<p align="center">
	<img src="./docs/images/Qbeast-spark.png" alt="Qbeast spark project"/>
</p>

<p align="center">
	<br/>
	<a href="./docs">Explore the Qbeast-spark docs >></a>
	<br/>
	<a href="./docs/sample_pushdown_demo.ipynb">See a notebook example >></a>
	<br/>
	<a href="https://join.slack.com/t/qbeast-users/shared_invite/zt-w0zy8qrm-tJ2di1kZpXhjDq_hAl1LHw">Slack</a> • <a href="https://blog.qbeast.io/">Medium</a> • <a href="https://qbeast.io">Website</a>
</p>

---
**Qbeast Spark** is an **open source** extension for **data lakehouse** that enables **multi-dimensional indexing** and **sampling pushdown**

<br/>

## Features

1. **Data Lakehouse** - Data lake with **ACID** properties, thanks to the underlying Delta Lake architecture


2. **Multi-column indexing**:  Filter your data with multiple columns using the Qbeast Format.
   

3. **Improved Sampling operator** - Efficiently reading statistically significant subsets. 
   

4. **Table Tolerance** - Model for sampling fraction and query accuracy trade-off. 


### What does it mean? - Let's see an example:

<div>
<img src="./docs/images/spark_delta_demo.gif" width="49%" alt="Demo for Delta format GIF" />

<img src="./docs/images/spark_qbeast_demo.gif" width="49%" alt="Demo for Qbeast format GIF" />
</div>

As you can see above, the Qbeast Spark extension allows **faster** queries with statistically **accurate** sampling.


| Format | Execution Time |   Result  |
|--------|:--------------:|:---------:|
| Delta  | ~ 2.5 min.     | 37.869383 |
| Qbeast | ~ 6.6 sec.     | 37.856333 |

In this example, **1% sampling** provides the result **x22 times faster** compared to using Delta format, with an **error of 0,034%**.

# Getting Started

>### Warning: DO NOT USE IN PRODUCTION!
> This project is in an early development phase: there are missing functionalities and the API might change drastically.
> 
> Join ⨝ the community to be a part of this project!
> 
> See Issues tab to know what is cooking 😎



# Setting up
### 1. Install [**sbt**(>=1.4.7)](https://www.scala-sbt.org/download.html).

### 2. Install **Spark**
Download **Spark 3.1.1 with Hadoop 3.2***, unzip it, and create the `SPARK_HOME` environment variable:<br />
*: You can use Hadoop 2.7 if desired, but you could have some troubles with different cloud providers' storage, read more about it [here](docs/CloudStorages.md).

```bash
wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz

tar xzvf spark-3.1.1-bin-hadoop3.2.tgz

export SPARK_HOME=$PWD/spark-3.1.1-bin-hadoop3.2
 ```

### 3. Project packaging:
Clone the repo, navigate to the repository folder, and package the project through **sbt**. [JDK 8](https://www.azul.com/downloads/?version=java-8-lts&package=jdk) is recommended.  

**Note**: You can specify **custom** Spark or Hadoop **versions** when packaging by using `-Dspark.version=3.2.0` or `-Dhadoop.version=2.7.4` when running `sbt assembly`.
If you have troubles with the versions you use, don't hesitate to **ask the community** in [GitHub discussions](https://github.com/Qbeast-io/qbeast-spark/discussions).

``` bash
git clone https://github.com/Qbeast-io/qbeast-spark.git

cd qbeast-spark

sbt assembly
```

# Quickstart
You can run the qbeast-spark application locally on your computer, or using a Docker image we already prepared with the dependencies.
You can find it in the [Packages section](https://github.com/orgs/Qbeast-io/packages?repo_name=qbeast-spark).

### 1. Launch a spark-shell

**Inside the project folder**, launch a **spark shell** with the required dependencies:

```bash
$SPARK_HOME/bin/spark-shell \
--jars ./target/scala-2.12/qbeast-spark-assembly-0.2.0.jar \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--packages io.delta:delta-core_2.12:1.0.0
```

### 2. Indexing a dataset

Read the CSV source file placed inside the project.

```scala
val csv_df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("./src/test/resources/ecommerce100K_2019_Oct.csv")
```

Indexing the dataset by writing it into the **qbeast** format, specifying the columns to index.

```scala
val tmp_dir = "/tmp/qbeast-spark"

csv_df.write
	.mode("overwrite")
	.format("qbeast")
	.option("columnsToIndex", "user_id,product_id")
	.save(tmp_dir)
```

###  3. Load the dataset
Load the newly indexed dataset.

```scala
val qbeast_df =
   spark
     .read
     .format("qbeast")
     .load(tmp_dir)
```

### 4. Examine the Query plan for sampling
Sampling the data, notice how the sampler is converted into filters and pushed down to the source!

```scala
qbeast_df.sample(0.1).explain(true)
```

Go to the [Quickstart](./docs/Quickstart.md) or [notebook](docs/sample_pushdown_demo.ipynb) for more details.

# Dependencies and Version Compatibility
|Version     |Spark       |Delta Lake  |sbt         |
|------------|:----------:|:----------:|:----------:|
|0.1.0       |=> 3.0.0    |=> 0.7.0    |=> 1.4.7    |

Check [here](https://docs.delta.io/latest/releases.html) for **Delta Lake** and **Apache Spark** version compatibility.  
**Note**: Different Spark and Hadoop versions can be specified when packaging the project. Read how to do it in the _Setting Up_ section.


# Contribution Guide

See [Contribution Guide](/CONTRIBUTING.md) for more information. 

# License
See [LICENSE](/LICENSE).

# Code of conduct

See [Code of conduct](/CODE_OF_CONDUCT.md)

