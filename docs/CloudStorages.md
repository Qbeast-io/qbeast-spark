# Recommendations for different Cloud Storage systems

## Content
In this document you will find recommendations on versions and configurations for different cloud providers' storage systems.
Note that some of them may have different requirements of libraries or configurations, so here we provide the ones
we use and we know they work.

###Hadoop versions
We currently support Hadoop 2.7 and 3.2 (recommended), so feel free to use any of them.
Nevertheless, if you use Hadoop 2.7 you'll need to add some **extra** configurations depending on the provider, which you can find below.
Note that some versions may not work for a cloud provider, so please read carefully.

### Configs for Hadoop 2.7
<details><summary>AWS S3</summary>
There's no known working version of Hadoop 2.7 for AWS S3. However, you can try to use it.<br />
Remember to include the following option if using Hadoop 2.7:<br />
<code>--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem</code>
</details>

<details><summary>Azure Blob Storage</summary>
- You can use this provider with Hadoop 2.7. To do so, you need to change the hadoop library to 2.7 (remember to change your Spark
installation as well):<br />
<code>org.apache.hadoop:hadoop-azure:2.7.4</code><br>
- In addition you must include the following config to use the _wasb_ filesystem:<br /><code>--conf spark.hadoop.fs.AbstractFileSystem.wasb.impl=org.apache.hadoop.fs.azure.Wasb</code>
</details>

## AWS S3
Amazon Web Services S3 does not work with Hadoop 2.7. For this provider you'll need Hadoop 3.2.

- If you are using a **public** bucket:
```bash
$SPARK_HOME/bin/spark-shell \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
--packages io.qbeast:qbeast-spark_2.12:0.2.0,\
io.delta:delta-core_2.12:1.0.0,\
com.amazonaws:aws-java-sdk:1.12.20,\
org.apache.hadoop:hadoop-common:3.2.0,\
org.apache.hadoop:hadoop-client:3.2.0,\
org.apache.hadoop:hadoop-aws:3.2.0
```
- If you are using **private** buckets:
```bash
$SPARK_HOME/bin/spark-shell \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
--conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--packages io.qbeast:qbeast-spark_2.12:0.2.0,\
io.delta:delta-core_2.12:1.0.0,\
org.apache.hadoop:hadoop-common:3.2.0,\
org.apache.hadoop:hadoop-client:3.2.0,\
org.apache.hadoop:hadoop-aws:3.2.0
```

## Azure Blob Storage
There are no known issues for this cloud provider when using qbeast-spark. You can use both Hadoop 2.7 and 3.2, but we
recommend using the latest. Remember that vanilla parquet format may not work in this type of storage.

- An example config setup follows:
```bash
$SPARK_HOME/bin/spark-shell \
--conf spark.hadoop.fs.azure.account.key.blobqsql.blob.core.windows.net="${AZURE_BLOB_STORAGE_KEY}" \
--conf spark.hadoop.fs.AbstractFileSystem.wasb.impl=org.apache.hadoop.fs.azure.Wasb \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--packages io.qbeast:qbeast-spark_2.12:0.2.0,\
io.delta:delta-core_2.12:1.0.0,\
org.apache.hadoop:hadoop-azure:3.2.0
```