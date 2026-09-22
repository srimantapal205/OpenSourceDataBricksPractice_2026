# Databricks notebook source
# DBTITLE 1,11 — Gold Aggregations
# MAGIC %md
# MAGIC # 11 — Gold Aggregations
# MAGIC
# MAGIC Aggregates `fact_databricks_cost` and `fact_audit_event` into all 7 Gold tables with day-over-day and MoM calculations. Uses MERGE for idempotent upserts on composite keys.

# COMMAND ----------

# DBTITLE 1,Aggregate fact tables into gold
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "gold_aggregations"
start_ts = datetime.now()

# ------------------------------------------------------------------
# 1. Gold Workspace Daily Cost (with day-over-day change)
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_daily AS
    WITH daily AS (
        SELECT usage_date, workspace_id, workspace_name, environment,
               SUM(usage_quantity) AS total_usage,
               SUM(estimated_cost) AS total_cost
          FROM finops.silver.fact_databricks_cost
         GROUP BY usage_date, workspace_id, workspace_name, environment
    ),
    with_prev AS (
        SELECT *, LAG(total_cost) OVER (PARTITION BY workspace_id ORDER BY usage_date) AS previous_day_cost
          FROM daily
    )
    SELECT usage_date, workspace_id, workspace_name, environment,
           total_usage, total_cost, previous_day_cost,
           total_cost - COALESCE(previous_day_cost, 0) AS cost_change,
           CASE WHEN previous_day_cost > 0
                THEN CAST(((total_cost - previous_day_cost) / previous_day_cost * 100) AS DECIMAL(10,2))
                ELSE NULL END AS cost_change_percentage
      FROM with_prev
""")
spark.sql("""
    MERGE INTO finops.gold.gold_workspace_daily_cost AS t
    USING vw_gold_daily AS s
    ON t.usage_date = s.usage_date AND t.workspace_id = s.workspace_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("1/7  gold_workspace_daily_cost done")

# ------------------------------------------------------------------
# 2. Gold Workspace Monthly Cost (with MoM change)
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_monthly AS
    WITH monthly AS (
        SELECT date_format(usage_date, 'yyyy-MM') AS month,
               workspace_id, workspace_name,
               SUM(usage_quantity) AS total_usage,
               SUM(estimated_cost) AS total_cost
          FROM finops.silver.fact_databricks_cost
         GROUP BY date_format(usage_date, 'yyyy-MM'), workspace_id, workspace_name
    ),
    with_prev AS (
        SELECT *, LAG(total_cost) OVER (PARTITION BY workspace_id ORDER BY month) AS previous_month_cost
          FROM monthly
    )
    SELECT month, workspace_id, workspace_name,
           total_usage, total_cost, previous_month_cost,
           CASE WHEN previous_month_cost > 0
                THEN CAST(((total_cost - previous_month_cost) / previous_month_cost * 100) AS DECIMAL(10,2))
                ELSE NULL END AS month_over_month_percentage
      FROM with_prev
""")
spark.sql("""
    MERGE INTO finops.gold.gold_workspace_monthly_cost AS t
    USING vw_gold_monthly AS s
    ON t.month = s.month AND t.workspace_id = s.workspace_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("2/7  gold_workspace_monthly_cost done")

# ------------------------------------------------------------------
# 3. Gold SKU Cost
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_sku AS
    SELECT usage_date, workspace_id, sku,
           SUM(usage_quantity) AS usage_quantity,
           SUM(estimated_cost) AS estimated_cost
      FROM finops.silver.fact_databricks_cost
     WHERE sku IS NOT NULL
     GROUP BY usage_date, workspace_id, sku
""")
spark.sql("""
    MERGE INTO finops.gold.gold_sku_cost AS t
    USING vw_gold_sku AS s
    ON t.usage_date = s.usage_date AND t.workspace_id = s.workspace_id AND t.sku = s.sku
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("3/7  gold_sku_cost done")

# ------------------------------------------------------------------
# 4. Gold User Cost
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_user AS
    SELECT usage_date, workspace_id, user_id AS user_name,
           SUM(usage_quantity) AS usage_quantity,
           SUM(estimated_cost) AS estimated_cost
      FROM finops.silver.fact_databricks_cost
     WHERE user_id IS NOT NULL
     GROUP BY usage_date, workspace_id, user_id
""")
spark.sql("""
    MERGE INTO finops.gold.gold_user_cost AS t
    USING vw_gold_user AS s
    ON t.usage_date = s.usage_date AND t.workspace_id = s.workspace_id AND t.user_name = s.user_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("4/7  gold_user_cost done")

# ------------------------------------------------------------------
# 5. Gold Job Cost
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_job AS
    SELECT usage_date, workspace_id, job_id, NULL AS job_name,
           SUM(usage_quantity) AS usage_quantity,
           SUM(estimated_cost) AS estimated_cost
      FROM finops.silver.fact_databricks_cost
     WHERE job_id IS NOT NULL
     GROUP BY usage_date, workspace_id, job_id
""")
spark.sql("""
    MERGE INTO finops.gold.gold_job_cost AS t
    USING vw_gold_job AS s
    ON t.usage_date = s.usage_date AND t.workspace_id = s.workspace_id AND t.job_id = s.job_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("5/7  gold_job_cost done")

# ------------------------------------------------------------------
# 6. Gold Cluster Cost
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_cluster AS
    SELECT usage_date, workspace_id, cluster_id, NULL AS cluster_name,
           SUM(usage_quantity) AS usage_quantity,
           SUM(estimated_cost) AS estimated_cost
      FROM finops.silver.fact_databricks_cost
     WHERE cluster_id IS NOT NULL
     GROUP BY usage_date, workspace_id, cluster_id
""")
spark.sql("""
    MERGE INTO finops.gold.gold_cluster_cost AS t
    USING vw_gold_cluster AS s
    ON t.usage_date = s.usage_date AND t.workspace_id = s.workspace_id AND t.cluster_id = s.cluster_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("6/7  gold_cluster_cost done")

# ------------------------------------------------------------------
# 7. Gold Audit Summary
# ------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_audit AS
    SELECT event_date, workspace_id, action_name, user_name,
           COUNT(*) AS event_count,
           COUNT(CASE WHEN response_status >= 400 THEN 1 END) AS failed_event_count
      FROM finops.silver.fact_audit_event
     GROUP BY event_date, workspace_id, action_name, user_name
""")
spark.sql("""
    MERGE INTO finops.gold.gold_audit_summary AS t
    USING vw_gold_audit AS s
    ON t.event_date = s.event_date AND t.workspace_id = s.workspace_id
       AND COALESCE(t.action_name, '__N__') = COALESCE(s.action_name, '__N__')
       AND COALESCE(t.user_name, '__N__') = COALESCE(s.user_name, '__N__')
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("7/7  gold_audit_summary done")

# ------------------------------------------------------------------
# Row counts and log
# ------------------------------------------------------------------
for tbl in ['gold_workspace_daily_cost','gold_workspace_monthly_cost','gold_sku_cost',
            'gold_user_cost','gold_job_cost','gold_cluster_cost','gold_audit_summary']:
    c = spark.sql(f"SELECT COUNT(*) FROM finops.gold.{tbl}").collect()[0][0]
    print(f"  {tbl}: {c} rows")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '11_gold_aggregations',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            0, 0, 0, 0, 'SUCCESS', NULL)
""")
print(f"Done. Duration={dur:.1f}s")