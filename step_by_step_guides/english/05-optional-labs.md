# Optional Labs

## Objective

In this section you will learn about Job and Resource ACL's, Iceberg WAP and Iceberg Compaction, in CDE.

## Table of Contents

1. [Lab 1: CDE Job and Resource ACLs](https://github.com/pdefusco/CDE_125_HOL/blob/main/step_by_step_guides/english/05-optional-labs.md#lab-1-cde-job-and-resource-acls).  
2. [Lab 2: ICEBERG COMPACTION with Spark in CDE](https://github.com/pdefusco/CDE_125_HOL/blob/main/step_by_step_guides/english/05-optional-labs.md#lab-1-cde-job-and-resource-acls).
3. [Lab 3: ICEBERG COMPACTION with Spark in CDE](https://github.com/pdefusco/CDE_125_HOL/blob/main/step_by_step_guides/english/05-optional-labs.md#lab-3-iceberg-wap-in-cloudera-data-engineering).

# Lab 1: CDE Job and Resource ACLs

This feature is only available in CDE in Private Cloud.

A resource in Cloudera Data Engineering is a named collection of files used by a job or a session. Resources can include application code, configuration files, custom Docker images, and Python virtual environment specifications (requirements.txt).

CDE 1.24 introduces granular ACL's to prevent developers from utilizing and modifying resources in use by other users' Spark and Airflow jobs. The attached examples rely on the CDE CLI.

### Background Info on CDE Resources

The resource types supported by Cloudera Data Engineering are files, python-env, and custom-runtime-image.

***files***
An arbitrary collection of files that a job can reference. The application code for the job, including any necessary configuration files or supporting libraries, can be stored in a files resource. Files can be uploaded to and removed from a resource as needed.

***python-env***
A defined virtual Python environment that a job runs in. The only file that can be uploaded to a python-env resource is a requirements.txt file. When you associate a python-env resource with a job, the job runs within a Python virtual environment built according to the requirements.txt specification.

***custom-runtime-image***
A Docker container image. When you run a job using a custom-runtime-image resource, the executors that are launched use your custom image.

### Background Info on User Access Management

User Access Management allows you to assign the roles and define who can access and manage the Cloudera Data Engineering environment, Virtual Clusters, and the artifacts by defining the access levels and permissions for a particular user.

Among other benefits, this brings CDE Admins and Users granular ACL's at the CDE Job and Resource level. CDE Users can leverage the CDE CLI to assign jobs and roles to specific roles in the cluster, thus preventing others from unexpectedly deleting or modifying spark pipelines unwantedly.

### Lab

Create a CDE Files Resource with ACL

```
cde resource create \
  --name filesResource \
  --type files \
  --acl-full-access-group group1 \            
  --acl-full-access-user user1 \    
  --acl-view-only-group group2 \               
  --acl-view-only-user user2
```

```
cde resource upload \
  --name myProperties \
  --local-path cde_jobs/propertiesFile_1.conf \
  --local-path cde_jobs/propertiesFile_2.conf \
  --local-path cde_jobs/sparkJob.py
```

Create a CDE Job with ACL

```
cde job create \
  --name sampleJob \
  --type spark \
  --mount-1-resource myProperties \
  --application-file sparkJob.py \
  --executor-cores 2 \
  --executor-memory "2g"
```

```
cde job run --name myPySparkJob\
--arg MY_DB\
--arg CUSTOMER_TABLE\
--arg propertiesFile_1.conf
```

### Summary

In Cloudera Data Engineering, Job ACLs and Resource ACLs control who can access and manage workloads. Job ACLs define which users or groups can view, run, edit, or delete data engineering jobs. Resource ACLs control access to underlying resources such as virtual clusters, databases, and compute resources.

Together, these ACLs help ensure that only authorized users can run jobs or use specific resources, improving security and governance in the platform.


# Lab 2: Iceberg Compaction with Spark in CDE

### Lab

#### Create Unevenly Distributed Data

Create CDE Depdendencies:

```
cde resource create \
  --name datagen-env \
  --type python-env

cde resource upload \
  --name datagen-env \
  --local-path compaction_resources/requirements.txt

cde resource create \
  --name files-spark35 \
  --type files

cde resource upload \
  --name files-spark35 \
  --local-path compaction_resources/datagen.py
```

Run DataGen Job:

```
cde job delete \
  --name datagen_1M

cde job create \
  --name datagen_1M \
  --python-env-resource-name datagen-env \
  --arg 25 \
  --arg 25 \
  --arg 1000000 \
  --arg db_1M \
  --arg source_1M \
  --type spark \
  --mount-1-resource files-spark35 \
  --application-file datagen.py \
  --executor-cores 4 \
  --executor-memory "4g"

cde job run \
  --name datagen_1M \
  --executor-memory "10g"
```

Next, we will use the data generated in order to create four Iceberg tables through a CDE Session. All tables will have the same data, but they will be partitioned differently according to different partitioning and bucketing schemes.

First, launch the CDE Session:

```
cde session create \
  --name compactionsession \
  --type pyspark \
  --driver-cores 4 \
  --driver-memory "8g" \
  --executor-cores 5 \
  --executor-memory "10g" \
  --num-executors 5
```

Next, in the CDE Session, validate table creation.

```
# Session Commands:

db_name = "db_1M"
src_tbl = "source_1M"
tgt_tbl = "target_1M"

spark.sql("drop table if exists {0}.{1}".format(db_name, tgt_tbl))
df = spark.sql("select * from {0}.{1}".format(db_name, src_tbl))

# Spark SQL Command:
print(spark.sql("SHOW CREATE TABLE {0}.{1}".format(db_name, src_tbl)).collect()[0][0])

#### Expected Output:
CREATE TABLE `db_1M`.`source_1M` (
  `unique_id` INT,
  `code` STRING,
  `col1` FLOAT,
  `col2` FLOAT,
  .
  .
  .
  `col99` FLOAT)
USING parquet
LOCATION 's3a://xxx/data/warehouse/tablespace/external/hive/db_1M.db/source_1M'
TBLPROPERTIES (
  'numFilesErasureCoded' = '0',
  'bucketing_version' = '2',
  'TRANSLATED_TO_EXTERNAL' = 'TRUE',
  'external.table.purge' = 'TRUE')
```

##### Example 1: Repartition the source table into a single partition, then bucket and sort by "unique_id"

This produced 25 evenly sized files of 64 MB each corresponding to 25 buckets. Each row is assigned into a bucket by means of a hashing function. On disk, the data will be written into files named "part-0000" but with 25 separate bucket ID's.

```
## PySpark Code in Session:

db_name = "db_1M"
src_tbl = "source_1M"
tgt_tbl = "ex_1"
buckets = 25

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 2: Repartition the source table into 25 partitions, then bucket and sort by "unique_id".

* This produces 25 partitions, where within each partition each row is assigned to a bucket by means of a hashing function.
* Notice we are not asking spark to repartition by a specific column. Spark just uses internal statistics to produce an as even as possible distribution of data among partitions.
* Depending on how data is distributed among each of the 25 partitions i.e. how skewed it is, Spark will hash each row based on "unique_id" and create bucket files within each partition.
* The max number of possible files created is 625 for perfectly evenly distributed data. In this case, each file is roughly 6MB.

```
## PySpark Code in Session:

db_name = "db_1M"
src_tbl = "source_1M"
tgt_tbl = "ex_2"
buckets = 25

df.repartition(25) \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 3: Repartition the source table based on a column with low cardinality and even distribution (i.e. low skew) and then bucket and sort with "unique_id".

* Generally the best approache among the examples so far, especially at larger scale. However, it requires identifying a column that has relatively low cardinality and even distribution.
* In this case this is "code"  which uniformly takes a value among a list of 9 string codes. But I know this because I generate the synthetic data. In your case you'd have to do some tests with your data.
This creates a maximum of 9 partitions x 25 buckets (225), reflecting the number of unique code categories and requested buckets.
* Notice ther heavy data skew between the different values.

Explore cardinality of the "Code" column:

```
## PySpark Code in Session:

db_name = "db_1M"
src_tbl = "source_1M"
tgt_tbl = "ex_3"
buckets = 25

spark.sql("""SELECT CODE, COUNT(*)
            FROM {0}.{1}
            GROUP BY CODE""".format(db_name, src_tbl)).show()

## Expected Output:
+----+--------+
|CODE|count(1)|
+----+--------+
|   g| 1819875|
|   f| 1817273|
|   e|  909405|
|   h| 1363761|
|   d|  911224|
|   c|  909449|
|   i|  907558|
|   b|  907035|
|   a|  454420|
+----+--------+
```

Now bucket your data by "unique_id", but repartition by "code":

```
df.repartition("code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 4: Repartition by evenly distributed column with low cardinality, but reduce number of partitions even further

* Because the number of files can reach the product of the number of partitions and the number of buckets, "bucketBy" can be very slow for large datasets where partitions are randomly arranged with respect to bucketing columns.
* Repartitioning can be dramatically faster when the number of partitions is set equal to the number of buckets, and the repartitioning key is made up of the bucketing columns. In other words, you will get the lowest number of files when you can align buckets and partitions i.e. having the repartitoning key made up of bucketing column(s).
* However, this is hard to achieve because the data may not lend itself to this. Regardless, testing different combinations of repartitioning and bucketing keys with the objective of getting to an as small as possible number of files, aiming at roughly 128 MB filesize, is an exercise that goes in the direction of this "ideal" scenario.  
* In the below example, the data is repartitioned by the low cardinality "code" column, as before, but this time the number of partitions is reduced to 5 resulting in 125 files i.e. a lower number of files each with more data than the previous example 3. Notice that the data skew in the "code" column leads to file size differences, with many files reaching ~40MB while others ~20MB or even as low as 7 MB in size.

```
# PySpark Code

db_name = "db_1M"
src_tbl = "source_1M"
tgt_tbl = "ex_4"
buckets = 25

df.repartition(5, "code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name+"." + tgt_tbl)
```

### Migrate Tables from Hive to Iceberg

Run the following commands in order to migrate the four tables to Iceberg without losing the Hive Table Physical Layout.

First, show location of each of the four Hive tables:

```
# List of table names you want to check
tables = [
    "db_1M.ex_1",
    "db_1M.ex_2",
    "db_1M.ex_3",
    "db_1M.ex_4"
]

# Loop through each table and run the query
for tbl in tables:
    print(f"\n=== Table: {tbl} ===")
    create_stmt = spark.sql("SHOW CREATE TABLE {}".format(tbl)).collect()[0][0]
    lines = create_stmt.split("\n")

    # Find the line containing LOCATION
    location_line = next((line for line in lines if "LOCATION" in line), None)

    if location_line:
        # Extract the path string by removing 'LOCATION' and quotes, and stripping whitespace
        table_location = location_line.replace("LOCATION", "").replace("'", "").strip()

    iceTableName = tbl.split(".")[1]
    iceTableName = "SPARK_CATALOG.DEFAULT." + iceTableName
    print(f"\n=== Migrating to Iceberg Table: {iceTableName} ===")
    print(f"\n Table Location: {table_location}")
    # Migrate Table to Iceberg:
    spark.sql("""
      CREATE TABLE {}
      USING iceberg
      LOCATION '{}'
    """.format(iceTableName, table_location))
```

### Look for Compaction Candidates

Now that we have four tables, which one is best suited for compaction?

Let's answer that by investigating how many files are found per partition, for each table?

```
# List of table names you want to check
tables = [
    "spark_catalog.default.ex_1",
    "spark_catalog.default.ex_2",
    "spark_catalog.default.ex_3",
    "spark_catalog.default.ex_4"
]

# Loop through each table and run the query
for tbl in tables:
    print(f"\n=== Table: {tbl} ===")
    query = f"""
        SELECT PARTITION, FILE_COUNT
        FROM {tbl}$partitions
    """
    spark.sql(query).show(truncate=False)
```

What is the size of each partition, for each table?

```
# Loop through each table and run the query
for tbl in tables:
    print(f"\n=== Table: {tbl} ===")
    query = f"""
        SELECT PARTITION, SUM(file_size_in_bytes) AS PARTITION_SIZE
        FROM {tbl}.FILES GROUP BY PARTITION
    """
    spark.sql(query).show(truncate=False)
```

```
spark.sql(
  """SELECT f.partition, f.file_path, s.snapshot_id, s.timestamp
  FROM spark_catalog.default.ex_1.files f
  JOIN spark_catalog.default.ex_1.snapshots s
  ON f.snapshot_id = s.snapshot_id"""
).show()
```

### Run Compaction

Now that we identified the table we must run compaction on, we can finally do so with the following call procedure:

```
from pyspark.sql import SparkSession

table_identifier = "<table_name_here>"

# Use SQL to call the procedure
spark.sql(f"""
  CALL my_catalog.system.rewrite_data_files(
    table => '{table_identifier}',
    strategy => 'binpack'
  )
""")
```

Now validate new file count per partition:

```
spark.sql("""
  SELECT PARTITION, FILE_COUNT FROM SPARK_CATALOG.TABLE.PARTITIONS
""").show()
```

## Summary

Cloudera Data Engineering (CDE) and the broader Cloudera Data Platform (CDP) offer a powerful, scalable solution for building, deploying, and managing data workflows in hybrid and multi-cloud environments. CDE simplifies data engineering with serverless architecture, auto-scaling Spark clusters, and built-in Apache Iceberg support. Combined with CDP’s enterprise-grade security, governance, and flexibility across public and private clouds, these platforms enable organizations to efficiently process large-scale data while maintaining compliance, agility, and control over their data ecosystems.


# Lab 3: Iceberg WAP in Cloudera Data Engineering

### Lab

#### Create CDE Files Resource

```
cde resource create --name myFiles --type files
cde resource upload --name myFiles --local-path resources/cell_towers_1.csv --local-path resources/cell_towers_2.csv
```

#### Launch CDE Session & Run Spark Commands

```
cde session create --name icebergSession --type pyspark --mount-1-resource myFiles
cde session interact --name icebergSession
```

##### Create Iceberg Table

```
USERNAME = "pauldefusco"

df  = spark.read.csv("/app/mount/cell_towers_1.csv", header=True, inferSchema=True)
df.writeTo("CELL_TOWERS_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

### Working with Iceberg Table Branches

##### Create Iceberg Table Branch

```
# CREATE TABLE BRANCH - Skip: Not supported
spark.sql("ALTER TABLE CELL_TOWERS_{} \
 CREATE BRANCH ingestion_branch \
 RETAIN 7 DAYS \
 WITH RETENTION 2 SNAPSHOTS;".format(USERNAME))

# SET TABLE BRANCH AS ACTIVE - Skip: Not supported
spark.sql("SET spark.wap.branch = 'ingestion_branch';")
```

##### Upsert Data into Branch with Iceberg Merge Into

```
# LOAD NEW TRANSACTION BATCH
batchDf = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
batchDf.printSchema()
batchDf.createOrReplaceTempView("BATCH_TEMP_VIEW".format(USERNAME))

# CREATE TABLE BRANCH - Supported
spark.sql("ALTER TABLE CELL_TOWERS_{} CREATE BRANCH ingestion_branch".format(USERNAME))
# WRITE DATA OPERATION ON TABLE BRANCH - Supported
batchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("CELL_TOWERS_{}".format(USERNAME))
```

Notice that a simple SELECT query against the table still returns the original data.

```
spark.sql("SELECT * FROM CELL_TOWERS_{};".format(USERNAME)).show()
```

If you want to access the data in the branch, you can specify the branch name in your SELECT query.

```
spark.sql("SELECT * FROM CELL_TOWERS_{} VERSION AS OF 'ingestion_branch';".format(USERNAME)).show()
```

Notice that a simple select against the table still returns the original data.

```
spark.sql("SELECT COUNT(*) FROM CELL_TOWERS_{};".format(USERNAME)).show()
```

### Cherrypicking Snapshots

The cherrypick_snapshot procedure creates a new snapshot incorporating the changes from another snapshot in a metadata-only operation (no new datafiles are created). To run the cherrypick_snapshot procedure you need to provide two parameters: the name of the table you’re updating as well as the ID of the snapshot the table should be updated based on. This transaction will return the snapshot IDs before and after the cherry-pick operation as source_snapshot_id and current_snapshot_id.

we will use the cherrypick operation to commit the changes to the table which were staged in the 'ingestion_branch' branch up until now.

```
# SHOW PAST BRANCH SNAPSHOT ID'S
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs;".format(USERNAME)).show()

# SAVE THE SNAPSHOT ID CORRESPONDING TO THE CREATED BRANCH
branchSnapshotId = spark.sql("SELECT snapshot_id FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs WHERE NAME == 'ingestion_branch';".format(USERNAME)).collect()[0][0]

# USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
# THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
spark.sql("CALL spark_catalog.system.cherrypick_snapshot('SPARK_CATALOG.DEFAULT.CELL_TOWERS_{0}',{1})".format(USERNAME, branchSnapshotId))

# VALIDATE THE CHANGES
# THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
spark.sql("SELECT COUNT(*) FROM CELL_TOWERS_{};".format(USERNAME)).show()
```

Track table snapshots post Merge Into operation:

```
# QUERY ICEBERG METADATA HISTORY TABLE
spark.sql("SELECT * FROM CELL_TOWERS_{}.snapshots".format(USERNAME)).show(20, False)
```

### Working with Iceberg Table Tags

##### Create Table Tag

Tags are immutable labels for Iceberg Snapshot ID's and can be used to reference a particular version of the table via a simple tag rather than having to work with Snapshot ID's directly.   

```
spark.sql("ALTER TABLE SPARK_CATALOG.DEFAULT.CELL_TOWERS_{} CREATE TAG businessOrg RETAIN 365 DAYS".format(USERNAME)).show()
```

Select your table snapshot as of a particular tag:

```
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{} VERSION AS OF 'businessOrg';".format(USERNAME)).show()
```

### Table Rollbacks

##### Rollback Table to previous Snapshot

Rollback table to state prior to merge into.

```
# SHOW LATEST TABLE HISTORY
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.HISTORY;".format(USERNAME)).show()

# SELECT SECOND LAST SNAPSHOT ID FROM HISTORY
priorSnapshotId = spark.sql("SELECT MIN(SNAPSHOT_ID) FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.HISTORY".format(USERNAME)).collect()[0][0]

# REPLACE ICEBERG SNAPSHOT ID WITH VALUE OBTAINED ABOVE
spark.sql("CALL SPARK_CATALOG.rollback_to_snapshot('DEFAULT.CELL_TOWERS_{0}', {1})".format(USERNAME, priorSnapshotId))

# VALIDATE ROLLBACK BY VERIFYING TABLE COUNT
spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}".format(USERNAME)).show()
```

### The refs Metadata Table

The refs metadata table helps you understand and manage your table’s snapshot history and retention policy, making it a crucial part of maintaining data versioning and ensuring that your table’s size is under control. Among its many use cases, the table provides a list of all the named references within an Iceberg table sich as Branch names and corresponding Snapshot ID's.

```
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs;".format(USERNAME)).show()
```

### Summary

CDE Provides native Iceberg support. Since the release of version 1.20, CDE introduces three Iceberg features that provide great benefits to Data Engineers.

1. Table Branching: ability to create independent lineages of snapshots, each with its own lifecycle.
2. Table Tagging: ability to tag an Iceberg table snapshot.
3. Table rollbacks: ability to technique reverse or “roll back” table to a previous state.

## Next Steps

Thank you and congratulations for making it all the way until the end!! Here are more helpful articles and blogs to continue your journey with Cloudera Data Engineering and Apache Iceberg:

- **Cloudera on Public Cloud 5-Day Free Trial**
   Experience Cloudera Data Engineering through common use cases that also introduce you to the platform’s fundamentals and key capabilities with predefined code samples and detailed step by step instructions.
   [Try Cloudera on Public Cloud for free](https://www.cloudera.com/products/cloudera-public-cloud-trial.html?utm_medium=sem&utm_source=google&keyplay=ALL&utm_campaign=FY25-Q2-GLOBAL-ME-PaidSearch-5-Day-Trial%20&cid=701Hr000001fVx4IAE&gad_source=1&gclid=EAIaIQobChMI4JnvtNHciAMVpAatBh2xRgugEAAYASAAEgLke_D_BwE)

- **Cloudera Blog: Supercharge Your Data Lakehouse with Apache Iceberg**  
   Learn how Apache Iceberg integrates with Cloudera Data Platform (CDP) to enable scalable and performant data lakehouse solutions, covering features like in-place table evolution and time travel.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/supercharge-your-data-lakehouse-with-apache-iceberg-in-cloudera-data-platform/)

- **Cloudera Docs: Using Apache Iceberg in Cloudera Data Engineering**  
   This documentation explains how Apache Iceberg is utilized in Cloudera Data Engineering to handle massive datasets, with detailed steps on managing tables and virtual clusters.  
   [Read more in Cloudera Documentation](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)

- **Cloudera Blog: Building an Open Data Lakehouse Using Apache Iceberg**  
   This article covers how to build and optimize a data lakehouse architecture using Apache Iceberg in CDP, along with advanced features like partition evolution and time travel queries.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/how-to-use-apache-iceberg-in-cdp-open-lakehouse/)
