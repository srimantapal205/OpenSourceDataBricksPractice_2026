# Databricks notebook source
# DBTITLE 1,09 — Data Quality
# MAGIC %md
# MAGIC # 09 — Data Quality Framework
# MAGIC
# MAGIC Runs automated DQ checks on billing, audit, and workspace tables. Logs results to `dq_execution_log` with PASS / WARN / FAIL status.

# COMMAND ----------

# DBTITLE 1,Run DQ checks and log results
import uuid
from datetime import datetime

batch_id  = str(uuid.uuid4())
dq_run_id = str(uuid.uuid4())
pipeline  = "data_quality"
start_ts  = datetime.now()

# ------------------------------------------------------------------
# Define DQ checks: (table_name, check_name, fail_query)
# ------------------------------------------------------------------
dq_checks = [
    ("finops.bronze.bronze_billing_usage", "missing_workspace_id",
     "SELECT COUNT(*) FROM finops.bronze.bronze_billing_usage WHERE workspace_id IS NULL"),
    ("finops.bronze.bronze_billing_usage", "missing_sku",
     "SELECT COUNT(*) FROM finops.bronze.bronze_billing_usage WHERE sku_name IS NULL"),
    ("finops.bronze.bronze_billing_usage", "negative_usage",
     "SELECT COUNT(*) FROM finops.bronze.bronze_billing_usage WHERE usage_quantity < 0"),
    ("finops.bronze.bronze_billing_usage", "zero_usage",
     "SELECT COUNT(*) FROM finops.bronze.bronze_billing_usage WHERE usage_quantity = 0"),
    ("finops.bronze.bronze_billing_usage", "duplicate_records",
     "SELECT COUNT(*) - COUNT(DISTINCT record_id) FROM finops.bronze.bronze_billing_usage"),
    ("finops.silver.fact_databricks_cost", "missing_price",
     "SELECT COUNT(*) FROM finops.silver.fact_databricks_cost WHERE unit_price IS NULL"),
    ("finops.silver.fact_databricks_cost", "missing_currency",
     "SELECT COUNT(*) FROM finops.silver.fact_databricks_cost WHERE currency IS NULL"),
    ("finops.bronze.bronze_audit_events", "missing_event_time",
     "SELECT COUNT(*) FROM finops.bronze.bronze_audit_events WHERE event_time IS NULL"),
    ("finops.bronze.bronze_audit_events", "missing_workspace_id",
     "SELECT COUNT(*) FROM finops.bronze.bronze_audit_events WHERE workspace_id IS NULL"),
    ("finops.bronze.bronze_audit_events", "missing_action_name",
     "SELECT COUNT(*) FROM finops.bronze.bronze_audit_events WHERE action_name IS NULL"),
    ("finops.bronze.bronze_audit_events", "duplicate_events",
     "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM finops.bronze.bronze_audit_events"),
    ("finops.dims.dim_workspace", "duplicate_workspace_ids",
     "SELECT COUNT(*) - COUNT(DISTINCT workspace_id) FROM finops.dims.dim_workspace"),
    ("finops.dims.dim_workspace", "missing_workspace_names",
     "SELECT COUNT(*) FROM finops.dims.dim_workspace WHERE workspace_name IS NULL"),
]

# ------------------------------------------------------------------
# Execute checks and collect results
# ------------------------------------------------------------------
rows = []
for table_name, check_name, fail_query in dq_checks:
    total  = spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]['c']
    failed = spark.sql(fail_query).collect()[0][0]
    if total == 0:
        status = 'WARN'
    elif failed == 0:
        status = 'PASS'
    elif failed > total * 0.1:
        status = 'FAIL'
    else:
        status = 'WARN'
    err = f"{failed} records failed" if failed > 0 else None
    rows.append((dq_run_id, datetime.now(), table_name, check_name,
                 int(total), int(failed), status, err))
    print(f"  [{status:4s}] {table_name.split('.')[-1]}.{check_name}: {failed}/{total}")

# ------------------------------------------------------------------
# Insert results into dq_execution_log
# ------------------------------------------------------------------
spark.createDataFrame(rows, schema="""
    dq_run_id STRING, execution_timestamp TIMESTAMP, table_name STRING,
    check_name STRING, total_records BIGINT, failed_records BIGINT,
    status STRING, error_message STRING
""").createOrReplaceTempView("vw_dq_results")

spark.sql("INSERT INTO finops.meta.dq_execution_log SELECT * FROM vw_dq_results")

passed = sum(1 for r in rows if r[6] == 'PASS')
warned = sum(1 for r in rows if r[6] == 'WARN')
failed = sum(1 for r in rows if r[6] == 'FAIL')
print(f"\nDQ Summary: {passed} PASS, {warned} WARN, {failed} FAIL")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '09_data_quality',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {len(rows)}, {passed}, {warned}, {failed}, 'SUCCESS', NULL)
""")
print(f"Done. Duration={dur:.1f}s")