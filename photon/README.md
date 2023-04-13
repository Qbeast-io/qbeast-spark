# Qbeast Photon Datasource

> This package is a standalone reader for Qbeast-Spark datasource. It does not include libraries for writing. 

The original purpose is to work with Databricks + Photon clusters without any compatibility problems. It will include `Sampling` and `Limit Pushdown` properties to enhance Photon Query Engine.

To use the library, you should follow the steps:

### 1. Package the code

You can do it with **`assembly`**, which **will include `core` in the jar**. 
 ```bash
 sbt project qbeastPhoton; sbt assembly
 ```

Or by **packaging both `photon` and `core` modules**:
 ```bash
 sbt project qbeastPhoton; sbt package
 sbt project qbeastCore; sbt package
 ```

### 2. Install the libraries in your Databricks cluster. 

### 3. Use init_scripts to enable extensions
Use `init_scripts` **to enable access to the customized `SessionExtension`**. The init script would move the jars of Qbeast-Core and Qbeast-Photon to `/databricks/jars` before initializing the cluster. This allows to load the `QbeastPhotonSessionExtension` which would convert the `BatchScan` into a `PhotonScan`. If you do not add the extensions, you can also benefit from Sampling Pushdown but the scan would need to be converted in runtime.
   
   **An example of the script**:
   ```bash
   #!/bin/bassh
   
   ## IF YOU USED ASSEMBLY
   cp /dbfs/FileStore/jars/qbeast_photon_2_12_0_4_0_assembly.jar /databricks/jars/
   
  ## OR IF YOU USED PACKAGE
   cp /dbfs/FileStore/jars/qbeast_core_2_12_0_3_3.jar /databricks/jars/
   cp /dbfs/FileStore/jars/qbeast_photon_2_12_0_4_0.jar /databricks/jars/
   
   ```
### 4. Add the following configuration to your Spark Application:
 ```bash 
 $SPARK_330/bin/spark-shell \
--packages io.delta:delta-core_2.12:2.1.0,io.qbeast:qbeast-core_2.12:0.3.3 \
--jars ./target/scala-2.12/qbeast-photon_2.12-0.1.0.jar \
--conf spark.sql.extensions=io.qbeast.spark.sql.QbeastPhotonSessionExtension
 ```
   
   In case of `spark.conf` file:
   ```bash 
   spark.databricks.cluster.profile singleNode
   spark.sql.extensions io.qbeast.spark.sql.QbeastPhotonSessionExtension
   spark.databricks.delta.formatCheck.enabled false
   spark.jars /databricks/jars/qbeast_photon_2_12_0_1_0.jar,/databricks/jars/qbeast_core_2_12_0_3_3.jar
   spark.master local[*, 4]
  ```

### 5. Read from qbeast-photon format any Qbeast written dataset.

   ```scala
   spark.read.format("qbeast").load("s3a:/path")
   ```

## Example

This is an example of how `qbeast` datasource could **pushdown** **Sampling** over the `QueryPlan`. 

### Qbeast Datasource
```scala
spark.read.format("qbeast").load("dbfs:/FileStore/tables/tpcds-1gb-qbeast/store_sales").sample(0.01).explain()

== Physical Plan ==
        *(1) ColumnarToRow
        +- PhotonResultStage
        +- PhotonScan parquet [ss_sold_date_sk#2269,ss_sold_time_sk#2270,ss_item_sk#2271,ss_customer_sk#2272,ss_cdemo_sk#2273,ss_hdemo_sk#2274,ss_addr_sk#2275,ss_store_sk#2276,ss_promo_sk#2277,ss_ticket_number#2278,ss_quantity#2279,ss_wholesale_cost#2280,ss_list_price#2281,ss_sales_price#2282,ss_ext_discount_amt#2283,ss_ext_sales_price#2284,ss_ext_wholesale_cost#2285,ss_ext_list_price#2286,ss_ext_tax#2287,ss_coupon_amt#2288,ss_net_paid#2289,ss_net_paid_inc_tax#2290,ss_net_profit#2291] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[dbfs:/FileStore/tables/tpcds-1gb-qbeast/store_sales/6647998c-cd72-4f8f..., PartitionFilters: [], ReadSchema: struct<ss_sold_date_sk:int,ss_sold_time_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_cdemo_sk:int,..., RequiredDataFilters: []


== Photon Explanation ==
        The query is fully supported by Photon.

```

### Delta Datasource

```scala

spark.read.format("delta").load("dbfs:/FileStore/tables/tpcds-1gb-qbeast/store_sales").sample(0.01).explain()
== Physical Plan ==
  *(1) Sample 0.0, 0.01, false, 8646570776163936710
+- *(1) ColumnarToRow
  +- PhotonResultStage
  +- PhotonScan parquet [ss_sold_date_sk#175,ss_sold_time_sk#176,ss_item_sk#177,ss_customer_sk#178,ss_cdemo_sk#179,ss_hdemo_sk#180,ss_addr_sk#181,ss_store_sk#182,ss_promo_sk#183,ss_ticket_number#184,ss_quantity#185,ss_wholesale_cost#186,ss_list_price#187,ss_sales_price#188,ss_ext_discount_amt#189,ss_ext_sales_price#190,ss_ext_wholesale_cost#191,ss_ext_list_price#192,ss_ext_tax#193,ss_coupon_amt#194,ss_net_paid#195,ss_net_paid_inc_tax#196,ss_net_profit#197] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/FileStore/tables/tpcds-1gb-qbeast/store_sales], PartitionFilters: [], ReadSchema: struct<ss_sold_date_sk:int,ss_sold_time_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_cdemo_sk:int,..., RequiredDataFilters: []


== Photon Explanation ==
  Photon does not fully support the query because:
  Unsupported node: Sample 0.0, 0.01, false, 8646570776163936710
reference node:
  Sample 0.0, 0.01, false, 8646570776163936710


```

