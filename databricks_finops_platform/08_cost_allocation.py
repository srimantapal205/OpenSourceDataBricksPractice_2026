# Databricks notebook source
# DBTITLE 1,08 — Cost Allocation
# MAGIC %md
# MAGIC # 08 — Cost Allocation
# MAGIC
# MAGIC Aggregates `fact_databricks_cost` by workspace metadata dimensions (cost center, business unit, environment, SKU) and MERGES into `fact_cost_allocation` using a deterministic hash key.

# COMMAND ----------

# DBTITLE 1,Aggregate cost into allocation fact
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "cost_allocation"
start_ts = datetime.now()

# ------------------------------------------------------------------
# Aggregate fact_databricks_cost by allocation dimensions
# ------------------------------------------------------------------
src = spark.sql("""
    SELECT
        md5(concat_ws('|',
            cast(f.usage_date as string),
            f.workspace_id,
            coalesce(f.cost_center, ''),
            coalesce(f.business_unit, ''),
            coalesce(f.sku, '')
        )) AS allocation_id,
        f.usage_date AS allocation_date,
        f.workspace_id, f.workspace_name,
        f.cost_center, f.business_unit,
        w.department,
        NULL AS project,
        NULL AS application,
        f.environment, f.cloud, f.sku,
        SUM(f.usage_quantity) AS usage_quantity,
        SUM(f.estimated_cost) AS estimated_cost,
        NULL AS job_id, NULL AS cluster_id, NULL AS warehouse_id, NULL AS user_id,
        'workspace_metadata' AS tag_source,
        current_timestamp() AS ingestion_timestamp
    FROM finops.silver.fact_databricks_cost f
    LEFT JOIN finops.dims.dim_workspace w ON f.workspace_id = w.workspace_id
    GROUP BY f.usage_date, f.workspace_id, f.workspace_name, f.cost_center, f.business_unit,
             w.department, f.environment, f.cloud, f.sku
""")
cnt = src.count()
print(f"Allocation records: {cnt}")
src.createOrReplaceTempView("vw_cost_alloc")

metrics = spark.sql("""
    MERGE INTO finops.silver.fact_cost_allocation AS t
    USING vw_cost_alloc AS s
    ON t.allocation_id = s.allocation_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").collect()[0]
ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
print(f"Inserted: {ins}  Updated: {upd}")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '08_cost_allocation',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")