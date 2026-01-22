# Spark and Iceberg Observability

![alt text](../../img/spark-obs-sol-arch.png)

## Objective

This repository stores a growing list of Spark Observability use cases leveraging CDP DataHub, Cloudera Data Engineering and Cloudera Observability.

Thoroughout the labs you will learn how to monitor, troubleshoot and improve your Spark applications following insights provided by Cloudera Observability.

## Motivation

Apache Spark is an open-source, fast, and powerful big data processing framework. It’s designed for large-scale data analytics and supports tasks like data transformation, machine learning, and real-time stream processing. In Cloudera you can run production Spark use cases at enterprise scale with Cloudera Data Engineering or Cloudera DataHub.

Cloudera Observability is a monitoring and troubleshooting tool for data pipelines and workloads running on the Cloudera Data Platform (CDP). It provides visibility into system performance, resource usage, and job health, helping data teams optimize performance, detect issues, and ensure efficient operation across hybrid and multi-cloud environments.

Apache Spark provides insights for monitoring and troubleshooting via the Spark UI. However, this is hard to use and is not great if you are running repetitive Spark pipelines. Cloudera Observability allows you to track how your Spark applications perform over time and provides automated recommendations so you can track performance trends and act accordingly.

## Requirements

You can run the labs in CDP DataHub, CDP Private Cloud Base (OnPrem) and CDE.

### Lab: CDE Iceberg Incremental Merge Into with Dynamic Skew

#### Lab Summary

In this lab you will monitor a Spark Iceberg Merge Into application that is run incrementally eight times via Airflow. At every run, the application creates a synthetic dataset to simulate a new batch load and performs an upsert against the same Iceberg table.

Every run's batch load is randomly generated with different skew, distribution, and cardinality. This is done on purpose in order to create abnormal executions that are flagged by Cloudera Observability. The synthetic data is generated randomly, but it is parameterized to increasingly perform more relative updates and less relative inserts at every execution.

When you reproduce the lab in your Virtual Cluster you will see different job runtimes and trends from what is shown in the below screenshots. However, you can still follow along to learn how to leverage the Observability UI in general.

If you'd like to review the code this is located in ```observability/iceberg_merge_skew_multikey_dynamic_incremental_random_overlap.py```.

#### Step 1: Set up the pipeline.

```
cde resource create \
  --name spark_observability_hol_user001

cde resource create \
  --name numpy-user001 \
  --type python-env

cde resource upload \
  --name numpy-user001 \
  --local-path requirements.txt

cde resource upload \
  --name spark_observability_hol-user001 \
  --local-path observability/iceberg_merge_skew_multikey_dynamic_incremental_random_overlap.py
```

Wait for the python environment build to complete. Then create the Incremental Read job.

![alt text](../../img/env-build-inprogress.png)

![alt text](../../img/env-build-complete.png)

```
cde job delete \
  --name iceberg_merge_dynamic_incremental_user001

cde job create \
  --name iceberg_merge_dynamic_incremental_user001 \
  --type spark \
  --application-file iceberg_merge_skew_multikey_dynamic_incremental_random_overlap.py \
  --python-env-resource-name numpy-user001 \
  --mount-1-resource spark_observability_hol_user001 \
  --executor-cores 4 \
  --executor-memory "8g" \
  --driver-cores 4 \
  --driver-memory "4g" \
  --arg spark_catalog.default.target_table_user001 \
  --arg spark_catalog.default.source_table_user001 \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.sql.adaptive.enabled=False \
  --conf spark.sql.shuffle.partitions=200
```

#### Step 2: Run the Pipeline.

Run the following commands to set up the Airflow pipeline. Once it's created, the Airflow job  will run the merge into jobs incrementally.

```
cde job delete \
  --name dynamic-incremental-orch-user001

cde resource upload \
  --name spark_observability_hol_user001 \
  --local-path code/airflow_orch.py

cde job create \
  --type airflow \
  --name dynamic-incremental-orch-user001 \
  --dag-file airflow_orch.py \
  --mount-1-resource spark_observability_hol_user001
```

![alt text](../../img/notebook-complete-cli.png)

Navigate to the CDE Job Runs page and validate that the Spark and Airflow jobs are running.

![alt text](../../img/jobs-running-1.png)

The Spark jobs pipeline will complete in about an hour. Once they have completed, you can move on to the next section.

![alt text](../../img/job-runs-complete.png)

#### Step 3: Navigate to the Observability UI and troubleshoot the pipeline.

In the Observability UI, locate your CDP Environment and drill down to your CDE Virtual Cluster expanding the tabs on the left side. In these screenshots the CDE Virtual Cluster is called "DEV". The click on "Spark" to land into the main Spark view.

![alt text](../../img/obs-1.png)

![alt text](../../img/obs-2.png)

![alt text](../../img/obs-3.png)

Scroll to the bottom right and notice your underperforming job runs are automatically flagged and categorized by warning type.

![alt text](../../img/obs-4.png)

Click on any of the job runs and explore the Overview tab. This is where all your insights for a particular run are summarized. Notice all warnings for this particular run are shown, in this example "Shuffle Task Input Skew".

![alt text](../../img/obs-5.png)

Open the "Health Checks" tab and investigate each of the stages flagged as anomalous. Notice on the right side a description of the flagged anomaly and a recommendation for fixing it are presented to you.

![alt text](../../img/obs-6.png)

![alt text](../../img/obs-7.png)

You can drill down further into the issue. Click on one of the flagged stages and review the Task Details view on the right side. A comparison with a baseline task is automatically shown for you. This indicates the particular Spark metrics that are anomalous when compared to the baseline.

![alt text](../../img/obs-8.png)

Expand the Baseline tab for a more refined comparison of Spark Metrics between the anomalous run and the baseline.  

![alt text](../../img/obs-9.png)

Navigate back to the main Jobs view and notice all runs are shown along with any potential Health Issues.

![alt text](../../img/obs-10.png)

Finally open the Trends tab and notice aggregate runtime metrics are shown. Cloudera Observability has automatically tracked every run of the Spark Application in question to monitor performance across time.

![alt text](../../img/obs-11.png)

## Summary & Next Steps

For Spark users, Cloudera Observability delivers unified, real-time and historical visibility into Spark workloads, enabling rapid root-cause analysis, proactive performance optimization, and resource cost governance to ensure stable, efficient execution of large-scale data pipelines on Cloudera Data Platform.

## Summary & Next Steps

For Spark users, Cloudera Observability delivers unified, real-time and historical visibility into Spark workloads, enabling rapid root-cause analysis, proactive performance optimization, and resource cost governance to ensure stable, efficient execution of large-scale data pipelines on Cloudera Data Platform.

**References & Further Reading**

#### Cloudera Observability

* **Beyond Monitoring: Introducing Cloudera Observability** – overview of observability’s role in CDP and performance/cost insights.
  [https://www.cloudera.com/blog/business/beyond-monitoring-introducing-cloudera-observability.html](https://www.cloudera.com/blog/business/beyond-monitoring-introducing-cloudera-observability.html) ([Cloudera][2])
* **Cloudera Observability product page** – feature and benefit summary for observability in CDP.
  [https://www.cloudera.com/products/cloudera-data-platform/observability.html](https://www.cloudera.com/products/cloudera-data-platform/observability.html) ([Cloudera][1])
* **Cloud Analytics Powered by FinOps** – how Cloudera Observability supports FinOps and workload performance optimization.
  [https://www.cloudera.com/blog/technical/cloud-analytics-powered-by-finops.html](https://www.cloudera.com/blog/technical/cloud-analytics-powered-by-finops.html) ([Cloudera][3])
* **One Big Cluster Stuck: Visibility and Transparency** – discusses visibility and troubleshooting using Cloudera Observability.
  [https://www.cloudera.com/blog/technical/one-big-cluster-stuck-visibility-and-transparency.html](https://www.cloudera.com/blog/technical/one-big-cluster-stuck-visibility-and-transparency.html) ([Cloudera][4])

#### Cloudera Data Engineering & Spark

* **Elevating Productivity: Cloudera Data Engineering & External IDE Connectivity for Spark** – Spark development productivity with CDE tooling.
  [https://www.cloudera.com/blog/technical/elevating-productivity-cloudera-data-engineering-brings-external-ide-connectivity-to-apache-spark.html](https://www.cloudera.com/blog/technical/elevating-productivity-cloudera-data-engineering-brings-external-ide-connectivity-to-apache-spark.html) ([Cloudera][5])
* **How Leading Data Teams Build AI-Ready Pipelines with Apache Iceberg and Spark** – real world use of Spark in modern data engineering at scale.
  [https://www.cloudera.com/blog/technical/how-leading-data-teams-build-ai-ready-pipelines-with-apache-iceberg-and-spark.html](https://www.cloudera.com/blog/technical/how-leading-data-teams-build-ai-ready-pipelines-with-apache-iceberg-and-spark.html) ([Cloudera][6])
* **Exploring Cloudera Data Engineering on CDP Public Cloud** – third-party overview of CDE and Spark management on CDP.
  [https://www.clearpeaks.com/exploring-cloudera-data-engineering-on-cdp-public-cloud/](https://www.clearpeaks.com/exploring-cloudera-data-engineering-on-cdp-public-cloud/) ([ClearPeaks][7])
* **Cloudera Data Engineering product page** – general overview of CDE and its Spark-centric pipeline capabilities.
  [https://www.cloudera.com/products/data-engineering.html](https://www.cloudera.com/products/data-engineering.html) ([Cloudera][8])

#### Spark Fundamentals

* **What Is Apache Spark? (Cloudera Resource)** – overview of Spark’s architecture and use cases.
  [https://www.cloudera.com/resources/faqs/apache-spark.html](https://www.cloudera.com/resources/faqs/apache-spark.html) ([Cloudera][9])


[1]: https://www.cloudera.com/products/cloudera-data-platform/observability.html?utm_source=chatgpt.com "Cloudera Observability | Cloudera"
[2]: https://www.cloudera.com/blog/business/beyond-monitoring-introducing-cloudera-observability.html?utm_source=chatgpt.com "Beyond Monitoring: Introducing Cloudera Observability | Blog | Cloudera"
[3]: https://www.cloudera.com/blog/technical/cloud-analytics-powered-by-finops.html?utm_source=chatgpt.com "Cloud Analytics Powered by FinOps | Blog - Cloudera"
[4]: https://www.cloudera.com/blog/technical/one-big-cluster-stuck-visibility-and-transparency.html?utm_source=chatgpt.com "One Big Cluster Stuck: Visibility and Transparency | Blog - Cloudera"
[5]: https://www.cloudera.com/blog/technical/elevating-productivity-cloudera-data-engineering-brings-external-ide-connectivity-to-apache-spark.html?utm_source=chatgpt.com "Cloudera Data Engineering Brings External IDE Connectivity to ..."
[6]: https://www.cloudera.com/blog/technical/how-leading-data-teams-build-ai-ready-pipelines-with-apache-iceberg-and-spark.html?utm_source=chatgpt.com "How Leading Data Teams Build AI-Ready Pipelines with Apache ..."
[7]: https://www.clearpeaks.com/exploring-cloudera-data-engineering-on-cdp-public-cloud/?utm_source=chatgpt.com "Exploring Cloudera Data Engineering on CDP Public Cloud"
[8]: https://www.cloudera.com/products/data-engineering.html?utm_source=chatgpt.com "Cloudera Data Engineering"
[9]: https://www.cloudera.com/resources/faqs/apache-spark.html?utm_source=chatgpt.com "What Is Apache Spark? | Cloudera"
