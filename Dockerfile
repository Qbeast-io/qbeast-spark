FROM gcr.io/spark-operator/spark:v3.1.1
USER root

ADD https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/2.0.0/azure-storage-2.0.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.2.0/hadoop-azure-3.2.0.jar \
  https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.8.0/azure-storage-blob-12.8.0.jar \
  https://repo1.maven.org/maven2/com/azure/azure-storage-common/12.8.0/azure-storage-common-12.8.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar \
  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/0.8.0/delta-core_2.12-0.8.0.jar \
  https://repo1.maven.org/maven2/com/google/guava/guava/18.0/guava-18.0.jar \
  /opt/spark/jars/ 

