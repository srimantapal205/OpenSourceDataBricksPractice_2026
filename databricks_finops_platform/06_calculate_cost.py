# Databricks notebook source
# DBTITLE 1,06 — Calculate Cost
# MAGIC %md
# MAGIC # 06 — Calculate Cost (Silver → Fact)
# MAGIC
# MAGIC Joins `silver_billing_usage` with `bronze_billing_prices` on SKU + cloud + price-validity window, calculates `unit_price` and lets the generated column `estimated_cost = usage_quantity × unit_price` compute the cost. MERGES into `fact_databricks_cost` on `cost_id`.

# COMMAND ----------

# DBTITLE 1,Silver → Fact cost calculation with pricing
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "calculate_cost"
start_ts = datetime.now()

# Columns for MERGE (excludes generated column estimated_cost)
cols = [
    "cost_id", "usage_date", "usage_start_time", "usage_end_time",
    "workspace_id", "workspace_name", "sku", "usage_unit", "usage_quantity",
    "unit_price", "currency", "record_type", "billing_origin_product",
    "job_id", "cluster_id", "warehouse_id", "user_id", "service_principal_id",
    "cost_center", "environment", "business_unit", "region", "cloud",
    "ingestion_timestamp"
]
col_list  = ", ".join(cols)
set_clause = ", ".join([f"t.{c} = s.{c}" for c in cols])
val_list  = ", ".join([f"s.{c}" for c in cols])

# ------------------------------------------------------------------
# Join silver billing with bronze prices (SKU + cloud + validity window)
# ------------------------------------------------------------------
src = spark.sql("""
    SELECT
        s.record_id AS cost_id,
        s.usage_date, s.usage_start_time, s.usage_end_time,
        s.workspace_id, s.workspace_name,
        s.sku, s.usage_unit, s.usage_quantity,
        COALESCE(p.pricing.effective_list.default, p.pricing.`default`) AS unit_price,
        p.currency_code AS currency,
        s.record_type, s.billing_origin_product,
        s.job_id, s.cluster_id, s.warehouse_id,
        s.user_id, s.service_principal_id,
        w.cost_center, s.environment, w.business_unit,
        s.region, s.cloud,
        current_timestamp() AS ingestion_timestamp
    FROM finops.silver.silver_billing_usage s
    LEFT JOIN finops.bronze.bronze_billing_prices p
      ON s.sku = p.sku_name
     AND s.cloud = p.cloud
     AND p.price_start_time <= s.usage_start_time
     AND (p.price_end_time >= s.usage_start_time OR p.price_end_time IS NULL)
    LEFT JOIN finops.dims.dim_workspace w ON s.workspace_id = w.workspace_id
""")
cnt = src.count()
print(f"Records to process: {cnt}")
src.createOrReplaceTempView("vw_fact_cost")

# MERGE with explicit columns (estimated_cost is auto-generated)
metrics = spark.sql(f"""
    MERGE INTO finops.silver.fact_databricks_cost AS t
    USING vw_fact_cost AS s
    ON t.cost_id = s.cost_id
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
""").collect()[0]
ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
print(f"Inserted: {ins}  Updated: {upd}")

# Quick cost summary
spark.sql("""
    SELECT
        COUNT(*) AS total_records,
        COUNT(CASE WHEN unit_price IS NULL THEN 1 END) AS missing_price,
        SUM(usage_quantity) AS total_dbus,
        SUM(estimated_cost)  AS total_estimated_cost
    FROM finops.silver.fact_databricks_cost
""").show()

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '06_calculate_cost',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")