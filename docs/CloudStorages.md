# Recommendations for different Cloud Storage systems

## Content
In this document you will find recommendations on versions and configurations for different cloud providers' storage systems.
Note that some of them may have different requirements of libraries or configurations, so here we provide the ones
we use and we know they work.

## AWS S3
Amazon Web Services S3 does not work with Hadoop 2.7. For this provider you'll need Hadoop 3.2.

- If you are using a **public** bucket:
```bash
$SPARK_HOME/bin/spark-shell \
  --jars ./target/scala-2.12/qbeast-spark-assembly-0.1.0.jar \
  --conf spark.sql.extensions=io.qbeast.spark.sql.QbeastSparkSessionExtension \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \ 
  --packages io.delta:delta-core_2.12:0.8.0,\
    com.amazonaws:aws-java-sdk:1.12.20,\
    org.apache.hadoop:hadoop-common:3.2.0,\
    org.apache.hadoop:hadoop-client:3.2.0,\
    org.apache.hadoop:hadoop-aws:3.2.0
```
- If you are using **private** buckets:
```bash
$SPARK_HOME/bin/spark-shell \
  --jars ./target/scala-2.12/qbeast-spark-assembly-0.1.0.jar \
  --conf spark.sql.extensions=io.qbeast.spark.sql.QbeastSparkSessionExtension \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --packages io.delta:delta-core_2.12:0.8.0,\
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
  --jars ./target/scala-2.12/qbeast-spark-assembly-0.1.0.jar \
  --conf spark.hadoop.fs.azure.account.key.blobqsql.blob.core.windows.net="${AZURE_BLOB_STORAGE_KEY}" \
  --conf spark.hadoop.fs.AbstractFileSystem.wasb.impl=org.apache.hadoop.fs.azure.Wasb \
  --conf spark.sql.extensions=io.qbeast.spark.sql.QbeastSparkSessionExtension
  --packages io.delta:delta-core_2.12:0.8.0,\
    org.apache.hadoop:hadoop-azure:3.2.0
```