# Databricks notebook source
# DBTITLE 1,04 — Ingest Audit Logs
# MAGIC %md
# MAGIC # 04 — Ingest Audit Logs (Bronze)
# MAGIC
# MAGIC Incrementally loads `system.access.audit` into `finops.bronze.bronze_audit_events` using MERGE on `event_id` for idempotency. Tracks the high-water mark on `event_time`.

# COMMAND ----------

# DBTITLE 1,Incremental MERGE from system.access.audit
import uuid
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

batch_id  = str(uuid.uuid4())
pipeline  = "ingest_audit_logs"
tbl       = "bronze_audit_events"
start_ts  = datetime.now()

# ------------------------------------------------------------------
# 1. Read watermark
# ------------------------------------------------------------------
wm = spark.sql(f"""
    SELECT COALESCE(MAX(last_watermark), TIMESTAMP('2020-01-01')) AS wm
    FROM finops.meta.pipeline_watermarks
    WHERE pipeline_name = '{pipeline}' AND table_name = '{tbl}'
""").collect()[0]['wm']
print(f"Watermark: {wm}")

# ------------------------------------------------------------------
# 2. Read incremental source from system.access.audit
# ------------------------------------------------------------------
src = spark.sql(f"""
    SELECT account_id, workspace_id, version, event_time, event_date,
           source_ip_address, user_agent, session_id, user_identity,
           service_name, action_name, request_id, request_params,
           response, audit_level, event_id, identity_metadata
      FROM system.access.audit
     WHERE event_time > TIMESTAMP('{wm}')
""")
cnt = src.count()
print(f"Records to process: {cnt}")

if cnt > 0:
    # 3. Add ingestion lineage
    src = (src
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_date",   current_timestamp().cast('date'))
        .withColumn("_source_system",    lit("system.access.audit"))
        .withColumn("_batch_id",         lit(batch_id))
    )
    src.createOrReplaceTempView("vw_new_audit")

    # 4. MERGE into bronze on event_id
    metrics = spark.sql("""
        MERGE INTO finops.bronze.bronze_audit_events AS t
        USING vw_new_audit AS s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
    print(f"Inserted: {ins}  Updated: {upd}")

    # 5. Advance watermark
    spark.sql(f"""
        MERGE INTO finops.meta.pipeline_watermarks AS t
        USING (SELECT '{pipeline}' AS pipeline_name, '{tbl}' AS table_name,
                      'event_time' AS watermark_column,
                      MAX(event_time) AS last_watermark,
                      CURRENT_DATE AS last_run_date,
                      CURRENT_TIMESTAMP AS updated_at
                 FROM vw_new_audit) AS s
        ON t.pipeline_name = s.pipeline_name AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    ins, upd = 0, 0

# 6. Log execution
end_ts = datetime.now()
dur   = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '04_ingest_audit_logs',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")