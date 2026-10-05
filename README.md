# OpenSourceDataBricksPractice_2026
Open Source DataBricks Practice 2026

A comprehensive, hands-on reference covering Databricks features with runnable code examples.

---

## Table of Contents

1. [Spark DataFrame Basics](#1-spark-dataframe-basics)
2. [Delta Lake Operations](#2-delta-lake-operations)
3. [SQL in Databricks](#3-sql-in-databricks)
4. [Auto Loader](#4-auto-loader)
5. [Structured Streaming](#5-structured-streaming)
6. [Window Functions](#6-window-functions)
7. [UDFs (User-Defined Functions)](#7-udfs-user-defined-functions)
8. [MLflow Tracking](#8-mlflow-tracking)
9. [Unity Catalog](#9-unity-catalog)
10. [Feature Store / Feature Engineering](#10-feature-store--feature-engineering)
11. [Visualization & Display](#11-visualization--display)
12. [Databricks Connect](#12-databricks-connect)
13. [Delta Live Tables (DLT)](#13-delta-live-tables-dlt)
14. [Jobs & Workflows](#14-jobs--workflows)
15. [Vector Search & RAG](#15-vector-search--rag)
16. [AI / GenAI Functions](#16-ai--genai-functions)
17. [Performance Tuning](#17-performance-tuning)
18. [Common Patterns & Snippets](#18-common-patterns--snippets)

---

## 1. Spark DataFrame Basics

```python
# Create a Spark session (auto-available in Databricks notebooks)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min, lit, when

spark = SparkSession.builder.appName("Practice2026").getOrCreate()

# --- Create a DataFrame from inline data ---
data = [
    (1, "Alice",   "Engineering", 75000,  "2020-01-15"),
    (2, "Bob",     "Marketing",   62000,  "2019-03-22"),
    (3, "Charlie", "Engineering", 80000,  "2021-07-01"),
    (4, "Diana",   "HR",          55000,  "2018-11-10"),
    (5, "Eve",     "Marketing",   68000,  "2022-05-18"),
]
cols = ["id", "name", "department", "salary", "hire_date"]
df = spark.createDataFrame(data, cols)

df.show()
df.printSchema()

# --- Select, filter, and transform ---
df.select("name", "salary").show()
df.filter(col("department") == "Engineering").show()
df.withColumn("salary_k", col("salary") / 1000).show()

# --- GroupBy aggregations ---
(
    df.groupBy("department")
      .agg(
          count("*").alias("headcount"),
          avg("salary").alias("avg_salary"),
          max("salary").alias("max_salary"),
      )
      .orderBy(col("avg_salary").desc())
      .show()
)

# --- Conditional logic with when/otherwise ---
df.withColumn(
    "band",
    when(col("salary") >= 75000, "High")
    .when(col("salary") >= 60000, "Medium")
    .otherwise("Low")
).show()

# --- Join two DataFrames ---
dept_data = [("Engineering", "Building A"), ("Marketing", "Building B"), ("HR", "Building C")]
dept_df = spark.createDataFrame(dept_data, ["department", "location"])

joined = df.join(dept_df, on="department", how="left")
joined.show()
```

---

## 2. Delta Lake Operations

```python
# --- Write a DataFrame as a Delta table ---
(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("demo.employees"))

# --- Read a Delta table ---
delta_df = spark.read.table("demo.employees")
delta_df.show(5)

# --- Upsert (MERGE) ---
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "demo.employees")

updates = spark.createDataFrame(
    [(3, "Charlie", "Engineering", 85000, "2021-07-01"),
     (6, "Frank",    "Sales",       59000, "2023-02-01")],
    cols
)

(delta_table.alias("target")
    .merge(updates.alias("src"), "target.id = src.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# --- Time travel ---
spark.read.format("delta").option("versionAsOf", 0).table("demo.employees").show()
spark.read.format("delta").option("timestampAsOf", "2024-01-01").table("demo.employees").show()

# --- Vacuum old files (retain 0 hours removes everything older than latest) ---
# spark.sql("VACUUM demo.employees RETAIN 0 HOURS")

# --- Delta table history ---
spark.sql("DESCRIBE HISTORY demo.employees").show(truncate=False)

# --- Optimize & Z-Order ---
spark.sql("OPTIMIZE demo.employees ZORDER BY (department)")
```

```sql
-- SQL equivalents
CREATE TABLE IF NOT EXISTS demo.employees (
  id         INT,
  name       STRING,
  department STRING,
  salary     INT,
  hire_date  STRING
) USING DELTA;

INSERT INTO demo.employees VALUES
  (7, 'Grace', 'Engineering', 72000, '2022-09-15');

-- Generate manifests for external engines
-- GENERATE symlink_format_manifest FOR TABLE demo.employees;
```

---

## 3. SQL in Databricks

```sql
-- Create a database and managed table
CREATE DATABASE IF NOT EXISTS practice_db;

CREATE TABLE IF NOT EXISTS practice_db.sales (
  order_id   INT,
  product    STRING,
  quantity   INT,
  price      DOUBLE,
  order_date DATE
) USING DELTA;

INSERT INTO practice_db.sales VALUES
  (1, 'Widget',  10, 19.99, '2024-01-10'),
  (2, 'Gadget',   5, 49.99, '2024-01-12'),
  (3, 'Widget',  20, 19.99, '2024-02-01'),
  (4, 'Gizmo',    8, 99.99, '2024-02-15');

-- CTE example
WITH revenue AS (
  SELECT product, SUM(quantity * price) AS total_revenue
  FROM practice_db.sales
  GROUP BY product
)
SELECT product, total_revenue,
       RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM revenue;

-- Temp views (session-scoped)
CREATE OR REPLACE TEMP VIEW recent_sales AS
SELECT * FROM practice_db.sales WHERE order_date >= '2024-02-01';

SELECT * FROM recent_sales;

-- MEDIAN / percentile
SELECT
  product,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
FROM practice_db.sales
GROUP BY product;
```

```python
# Run SQL from Python cell
result = spark.sql("SELECT product, SUM(quantity * price) AS revenue FROM practice_db.sales GROUP BY product ORDER BY revenue DESC")
result.show()
```

---

## 4. Auto Loader

```python
# Incrementally ingest JSON/CSV files from cloud storage into a Delta table
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema/employees_json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://my-bucket/raw/employees/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoint/streams/employees_json")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)        # run once per job trigger
    .toTable("demo.employees_autoloader"))
```

```sql
-- SQL equivalent
CREATE OR REFRESH STREAMING TABLE demo.employees_autoloader;

CREATE FLOW employees_flow AS
  COPY INTO demo.employees_autoloader
  FROM 's3://my-bucket/raw/employees/'
  FILEFORMAT = JSON;
```

---

## 5. Structured Streaming

```python
from pyspark.sql.functions import window, current_timestamp

# Read from a Kafka topic
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse value as JSON and aggregate in 1-minute windows
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType, IntegerType

schema = StructType() \
    .add("event_type", StringType()) \
    .add("user_id", IntegerType())

parsed = kafka_stream.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

windowed = (
    parsed
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(current_timestamp(), "1 minute"), "event_type")
    .agg(count("*").alias("event_count"))
)

# Write to console (for testing) — use .toTable() in production
query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

# query.awaitTermination()
```

---

## 6. Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

w_spec = Window.partitionBy("department").orderBy(col("salary").desc())

(df
    .withColumn("row_num",    row_number().over(w_spec))
    .withColumn("rank_val",   rank().over(w_spec))
    .withColumn("dense_rank",  dense_rank().over(w_spec))
    .withColumn("prev_salary", lag("salary").over(w_spec))
    .withColumn("next_salary", lead("salary").over(w_spec))
    .show())

# Running total within each department
running_spec = Window.partitionBy("department").orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("salary").over(running_spec)).show()
```

```sql
-- SQL equivalent
SELECT
  name, department, salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dr,
  LAG(salary, 1) OVER (PARTITION BY department ORDER BY salary DESC) AS prev_sal,
  SUM(salary) OVER (PARTITION BY department ORDER BY id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM demo.employees;
```

---

## 7. UDFs (User-Defined Functions)

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

# --- Standard Python UDF (slower) ---
@udf(returnType=StringType())
def grade_salary(salary):
    if salary is None:
        return "Unknown"
    if salary >= 75000:
        return "High"
    elif salary >= 60000:
        return "Medium"
    return "Low"

df.withColumn("grade", grade_salary(col("salary"))).show()

# --- Vectorized Pandas UDF (much faster) ---
@pandas_udf(DoubleType())
def normalize_salary(s: pd.Series) -> pd.Series:
    return (s - s.min()) / (s.max() - s.min())

df.withColumn("salary_norm", normalize_salary(col("salary"))).show()

# --- SQL UDF registered from Python ---
spark.udf.register("grade_salary_sql", grade_salary)
spark.sql("SELECT name, salary, grade_salary_sql(salary) AS grade FROM demo.employees").show()
```

```sql
-- Pure SQL UDF (recommended — no serialization overhead)
CREATE OR REPLACE FUNCTION practice_db.grade_salary(salary DOUBLE)
RETURNS STRING
RETURN
  CASE
    WHEN salary >= 75000 THEN 'High'
    WHEN salary >= 60000 THEN 'Medium'
    ELSE 'Low'
  END;

SELECT name, salary, practice_db.grade_salary(CAST(salary AS DOUBLE)) AS grade
FROM demo.employees;
```

---

## 8. MLflow Tracking

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd

# Prepare data
pdf = df.toPandas()
X = pdf[["id"]]
y = pdf["salary"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Start an MLflow run
mlflow.set_experiment("/Shared/salary_prediction")

with mlflow.start_run(run_name="rf_baseline") as run:
    params = {"n_estimators": 100, "max_depth": 5, "random_state": 42}
    mlflow.log_params(params)

    model = RandomForestRegressor(**params)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    mlflow.log_metric("rmse", mean_squared_error(y_test, preds, squared=False))
    mlflow.log_metric("r2", r2_score(y_test, preds))

    mlflow.sklearn.log_model(model, "model", registered_model_name="salary_rf")

    print(f"Run ID: {run.info.run_id}")

# Load a registered model
loaded = mlflow.pyfunc.load_model(f"models:/salary_rf/latest")
print(loaded.predict(X_test))
```

```sql
-- Query MLflow system tables for run history
SELECT
  experiment_id, run_id, status, metrics.rmse, metrics.r2
FROM system.mlflow.triples
WHERE experiment_name = '/Shared/salary_prediction'
ORDER BY metrics.rmse ASC;
```

---

## 9. Unity Catalog

```sql
-- Create a catalog and schema
CREATE CATALOG IF NOT EXISTS practice_cat;
USE CATALOG practice_cat;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant access
GRANT USE SCHEMA ON SCHEMA practice_cat.silver TO `analyst_group`;
GRANT SELECT ON TABLE practice_cat.silver.employees TO `analyst_group`;

-- Add a column mask (PII protection)
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CONCAT('***@', SPLIT_PART(email, '@', 2));

ALTER TABLE practice_cat.silver.customers
  ALTER COLUMN email SET MASK mask_email;

-- Row-level security with a filter
CREATE OR REPLACE FUNCTION region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_member('region_us') AND region = 'US';

ALTER TABLE practice_cat.silver.sales
  SET ROW FILTER region_filter ON (region);

-- Tags for governance
ALTER TABLE practice_cat.silver.employees
  SET TAGS ('pii' = 'true', 'domain' = 'hr');
```

```python
# List catalogs/schemas/tables via the Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
for cat in w.catalogs.list():
    print(cat.name)
```

---

## 10. Feature Store / Feature Engineering

```python
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# Create a feature table
fe.create_table(
    name="practice_cat.silver.employee_features",
    primary_keys=["id"],
    df=df.withColumn("tenure_years", lit(3)),
    schema=df.withColumn("tenure_years", lit(3)).schema,
    description="Employee features for salary prediction",
)

# Build a training set with point-in-time join
training_set = fe.create_training_set(
    df=df.select("id", "salary"),
    feature_lookups=[
        FeatureLookup(
            table_name="practice_cat.silver.employee_features",
            lookup_key="id",
            feature_names=["tenure_years", "salary"],
        )
    ],
    label="salary",
    exclude_columns=["id"],
)

train_df = training_set.load_df()
train_df.show()
```

---

## 11. Visualization & Display

```python
# Databricks built-in display (renders interactive charts)
display(df.groupBy("department").agg(avg("salary").alias("avg_salary")))

# Matplotlib / Seaborn in a notebook
import matplotlib.pyplot as plt
import seaborn as sns

pdf = df.toPandas()
fig, ax = plt.subplots(figsize=(8, 4))
sns.barplot(data=pdf, x="department", y="salary", ax=ax)
ax.set_title("Average Salary by Department")
plt.tight_layout()
plt.show()
```

```python
# Plotly interactive chart
import plotly.express as px

fig = px.bar(pdf, x="department", y="salary", color="name", title="Salary by Department")
fig.show()
```

---

## 12. Databricks Connect

```python
# Install: pip install databricks-connect
# Configure: databricks-connect configure

# Once configured, any Spark code run locally uses the remote Databricks cluster
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df_local = spark.read.table("practice_cat.silver.employees")
df_local.show()

# Submit a local .py script to the remote cluster
# databricks-connect submit_script.py
```

---

## 13. Delta Live Tables (DLT)

```python
# DLT pipeline definition (run as a DLT notebook, not a standard notebook)
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(comment="Raw ingested employee data")
def bronze_employees():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://my-bucket/raw/employees/")
    )

@dlt.table(comment="Cleaned and deduplicated")
@dlt.expect_or_drop("valid_salary", "salary > 0")
@dlt.expect_or_drop("valid_name",  "name IS NOT NULL")
def silver_employees():
    return (
        dlt.read("bronze_employees")
        .dropDuplicates(["id"])
        .withColumn("ingested_at", current_timestamp())
    )

@dlt.table(comment="Aggregated department metrics")
def gold_dept_metrics():
    return (
        dlt.read("silver_employees")
        .groupBy("department")
        .agg(
            count("*").alias("headcount"),
            avg("salary").alias("avg_salary"),
        )
    )
```

```sql
-- SQL DLT equivalent
CREATE OR REFRESH STREAMING TABLE bronze_employees AS
  SELECT * FROM cloud_files('s3://my-bucket/raw/employees/', 'json');

CREATE OR REFRESH MATERIALIZED VIEW silver_employees AS
  SELECT DISTINCT *, current_timestamp() AS ingested_at
  FROM STREAM(bronze_employees)
  WHERE salary > 0 AND name IS NOT NULL;

CREATE OR REFRESH MATERIALIZED VIEW gold_dept_metrics AS
  SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
  FROM silver_employees
  GROUP BY department;
```

---

## 14. Jobs & Workflows

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, JobCluster

w = WorkspaceClient()

job = w.jobs.create(
    name="daily_employee_etl",
    tasks=[{
        "task_key": "etl_task",
        "notebook_task": {
            "notebook_path": "/Workspace/Practice/employee_etl"
        },
        "job_cluster": {"job_cluster_key": "shared_cluster"},
    }],
    job_clusters=[{
        "job_cluster_key": "shared_cluster",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        },
    }],
    schedule={
        "quartz_cron_expression": "0 0 8 * * ?",   # daily at 8 AM UTC
        "timezone_id": "UTC",
    },
)

print(f"Created job {job.job_id}")
```

```sql
-- Monitor job runs via system tables
SELECT
  job_id, run_id, result_state, start_time, end_time,
  TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_s
FROM system.lakeflow.job_runs
WHERE job_name = 'daily_employee_etl'
ORDER BY start_time DESC
LIMIT 20;
```

---

## 15. Vector Search & RAG

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# Create a vector search endpoint
endpoint = vsc.create_endpoint(
    name="practice_vs_endpoint",
    endpoint_type="STANDARD",
)

# Create a Delta Sync index
index = vsc.create_delta_sync_index(
    endpoint_name="practice_vs_endpoint",
    index_name="practice_cat.gold.employee_embeddings",
    source_table_name="practice_cat.gold.employees_with_embeddings",
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column="profile_text",
    embedding_model_endpoint_name="databricks-gte-large-en",
)

# Similarity search with a filter
results = index.similarity_search(
    query_text="Python engineer with 5 years experience",
    columns=["id", "name", "department"],
    num_results=5,
    filters={"department": "Engineering"},
)
print(results)
```

---

## 16. AI / GenAI Functions

```sql
-- AI functions available directly in SQL
SELECT
  ai_analyze_sentiment('Databricks is amazing!') AS sentiment,
  ai_extract('My email is user@example.com', 'email') AS email,
  ai_summarize('This is a long text that needs to be summarized into a short sentence.') AS summary;

-- Use an external LLM endpoint
SELECT ai_query(
  'my_llm_endpoint',
  'Explain Delta Lake in one sentence.'
) AS explanation;
```

```python
# Foundation Model API in Python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="databricks-dbrx-instruct",
    messages=[ChatMessage(role=ChatMessageRole.USER, content="What is Unity Catalog?")],
    max_tokens=200,
)
print(response.message.content)
```

```python
# LangChain integration with Databricks models
from langchain_databricks import ChatDatabricks

llm = ChatDatabricks(endpoint="databricks-dbrx-instruct", temperature=0.3)
response = llm.invoke("Write a SQL query to count employees per department.")
print(response.content)
```

---

## 17. Performance Tuning

```sql
-- Enable predictive optimization at catalog level
ALTER CATALOG practice_cat ENABLE PREDICTIVE OPTIMIZATION;

-- Liquid Clustering (replaces partitioning + ZORDER)
CREATE OR REPLACE TABLE practice_cat.silver.orders (
  order_id   BIGINT,
  customer_id STRING,
  order_date  DATE,
  amount      DOUBLE
) USING DELTA
CLUSTER BY (customer_id, order_date);
```

```python
# Broadcast join for small-to-large joins
from pyspark.sql.functions import broadcast

large_df = spark.read.table("practice_cat.brown.orders")
small_df = spark.read.table("practice_cat.brown.customers")

joined = large_df.join(broadcast(small_df), on="customer_id", how="inner")

# Cache a frequently used DataFrame
joined.cache()
joined.count()  # materialize
```

```sql
-- Inspect query plan & metrics
EXPLAIN FORMATTED
  SELECT department, AVG(salary) FROM demo.employees GROUP BY department;

-- View query history from system tables
SELECT
  query_id, query_text, total_time_ms, rows_read,
  bytes_read, total_time_ms AS duration_ms
FROM system.query.history
WHERE statement_text LIKE '%employees%'
ORDER BY start_time DESC
LIMIT 10;
```

---

## 18. Common Patterns & Snippets

```python
# --- Read from external sources ---
# S3
df_s3 = spark.read.parquet("s3://my-bucket/data/")
# ADLS
df_adls = spark.read.parquet("abfss://container@acct.dfs.core.windows.net/data/")
# GCS
df_gcs = spark.read.parquet("gs://my-bucket/data/")
# JDBC
jdbc_df = (spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/db")
    .option("dbtable", "public.employees")
    .option("user", "user")
    .option("password", dbutils.secrets.get("my-scope", "pg_pwd"))
    .load())

# --- Write with partitioning ---
(df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("department")
    .saveAsTable("practice_cat.silver.employees_partitioned"))

# --- Secrets ---
secret = dbutils.secrets.get(scope="my-scope", key="api-key")
# secret value is redacted in output for security

# --- Widgets for parameterized notebooks ---
dbutils.widgets.text("dept", "Engineering", "Department")
dept_val = dbutils.widgets.get("dept")

spark.sql(f"SELECT * FROM demo.employees WHERE department = '{dept_val}'").show()

# --- Run a notebook from another notebook ---
# dbutils.notebook.run("/Workspace/Practice/helper_notebook", 60, {"dept": "Engineering"})

# --- Schema inference + evolution ---
raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/checkpoint/csv_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load("s3://my-bucket/csv/")
    .writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", "/checkpoint/csv_stream")
    .toTable("practice_cat.bronze.csv_data"))

# --- Delta Clone (shallow / deep) ---
spark.sql("CREATE TABLE practice_cat.silver.employees_clone DEEP CLONE practice_cat.silver.employees")
spark.sql("CREATE TABLE practice_cat.bronze.employees_shallow SHALLOW CLONE practice_cat.silver.employees")

# --- Approximate distinct count ---
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("department", rsd=0.05).alias("approx_dept_count")).show()
```

---

> **Tips**
> * Use `display(df)` for interactive charts and data profiles.
> * Prefer **Delta Lake** tables over Parquet for ACID + time travel.
> * Use **Auto Loader** for incremental file ingestion instead of batch reads.
> * Register Python functions as **SQL UDFs** when possible for performance.
> * Leverage **Unity Catalog** for all governance, lineage, and access control.
> * Monitor costs and performance with **system tables**.
