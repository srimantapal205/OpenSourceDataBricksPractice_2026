# Databricks notebook source
# DBTITLE 1,10 — Anomaly Detection
# MAGIC %md
# MAGIC # 10 — Cost Anomaly Detection
# MAGIC
# MAGIC Detects daily cost spikes and SKU cost spikes by comparing actual costs to a rolling 7-day baseline. Classifies anomalies by severity (Critical / High / Medium / Low) and MERGES into `fact_cost_anomaly`.

# COMMAND ----------

# DBTITLE 1,Detect cost anomalies with rolling baseline
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "anomaly_detection"
start_ts = datetime.now()

# 1. Daily cost spike detection (7-day rolling baseline)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_daily_cost AS
    SELECT usage_date, workspace_id, workspace_name,
           SUM(estimated_cost) AS daily_cost,
           SUM(usage_quantity) AS daily_dbu
      FROM finops.silver.fact_databricks_cost
     GROUP BY usage_date, workspace_id, workspace_name
""")

daily_anom = spark.sql("""
    WITH baseline AS (
        SELECT d.usage_date, d.workspace_id, d.workspace_name, d.daily_cost,
               AVG(d2.daily_cost) AS baseline_cost
          FROM vw_daily_cost d
          LEFT JOIN vw_daily_cost d2
            ON d.workspace_id = d2.workspace_id
           AND d2.usage_date >= d.usage_date - INTERVAL 7 DAYS
           AND d2.usage_date <  d.usage_date
         GROUP BY d.usage_date, d.workspace_id, d.workspace_name, d.daily_cost
    )
    SELECT
        md5(concat_ws('|', cast(usage_date as string), workspace_id, 'daily_cost_spike')) AS anomaly_id,
        usage_date AS detection_date, workspace_id, workspace_name,
        'daily_cost_spike' AS anomaly_type, 'daily_cost' AS metric,
        baseline_cost AS baseline_value, daily_cost AS actual_value,
        CASE WHEN baseline_cost > 0
             THEN CAST(((daily_cost - baseline_cost) / baseline_cost * 100) AS DECIMAL(10,2))
             ELSE NULL END AS deviation_percentage,
        CASE WHEN baseline_cost > 0 AND daily_cost > baseline_cost * 2   THEN 'Critical'
             WHEN baseline_cost > 0 AND daily_cost > baseline_cost * 1.5  THEN 'High'
             WHEN baseline_cost > 0 AND daily_cost > baseline_cost * 1.25 THEN 'Medium'
             ELSE 'Low' END AS severity,
        current_timestamp() AS detected_timestamp, 'Open' AS status
    FROM baseline
    WHERE baseline_cost > 0 AND daily_cost > baseline_cost * 1.25
""")
cnt1 = daily_anom.count()
print(f"Daily cost anomalies: {cnt1}")
daily_anom.createOrReplaceTempView("vw_daily_anom")
m1 = spark.sql("""
    MERGE INTO finops.silver.fact_cost_anomaly AS t
    USING vw_daily_anom AS s
    ON t.anomaly_id = s.anomaly_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").collect()[0]

# 2. SKU cost spike detection (7-day rolling baseline)
sku_anom = spark.sql("""
    WITH sku_daily AS (
        SELECT usage_date, workspace_id, workspace_name, sku,
               SUM(estimated_cost) AS daily_cost
          FROM finops.silver.fact_databricks_cost
         GROUP BY usage_date, workspace_id, workspace_name, sku
    ),
    sku_baseline AS (
        SELECT s.usage_date, s.workspace_id, s.workspace_name, s.sku, s.daily_cost,
               AVG(s2.daily_cost) AS baseline_cost
          FROM sku_daily s
          LEFT JOIN sku_daily s2
            ON s.workspace_id = s2.workspace_id AND s.sku = s2.sku
           AND s2.usage_date >= s.usage_date - INTERVAL 7 DAYS
           AND s2.usage_date <  s.usage_date
         GROUP BY s.usage_date, s.workspace_id, s.workspace_name, s.sku, s.daily_cost
    )
    SELECT
        md5(concat_ws('|', cast(usage_date as string), workspace_id, sku, 'sku_spike')) AS anomaly_id,
        usage_date AS detection_date, workspace_id, workspace_name,
        'sku_spike' AS anomaly_type, 'sku_daily_cost' AS metric,
        baseline_cost AS baseline_value, daily_cost AS actual_value,
        CASE WHEN baseline_cost > 0
             THEN CAST(((daily_cost - baseline_cost) / baseline_cost * 100) AS DECIMAL(10,2))
             ELSE NULL END AS deviation_percentage,
        CASE WHEN baseline_cost > 0 AND daily_cost > baseline_cost * 2   THEN 'Critical'
             WHEN baseline_cost > 0 AND daily_cost > baseline_cost * 1.5  THEN 'High'
             WHEN baseline_cost > 0 AND daily_cost > baseline_cost * 1.25 THEN 'Medium'
             ELSE 'Low' END AS severity,
        current_timestamp() AS detected_timestamp, 'Open' AS status
    FROM sku_baseline
    WHERE baseline_cost > 0 AND daily_cost > baseline_cost * 1.25
""")
cnt2 = sku_anom.count()
print(f"SKU cost anomalies: {cnt2}")
sku_anom.createOrReplaceTempView("vw_sku_anom")
m2 = spark.sql("""
    MERGE INTO finops.silver.fact_cost_anomaly AS t
    USING vw_sku_anom AS s
    ON t.anomaly_id = s.anomaly_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").collect()[0]

ins = m1['num_inserted_rows'] + m2['num_inserted_rows']
upd = m1['num_updated_rows'] + m2['num_updated_rows']
print(f"Inserted: {ins}  Updated: {upd}")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '10_anomaly_detection',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt1 + cnt2}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Anomalies={cnt1 + cnt2}  Duration={dur:.1f}s")