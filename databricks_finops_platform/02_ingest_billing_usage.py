# Databricks notebook source
# DBTITLE 1,02 — Ingest Billing Usage
# MAGIC %md
# MAGIC # 02 — Ingest Billing Usage (Bronze)
# MAGIC
# MAGIC Incrementally loads `system.billing.usage` into `finops.bronze.bronze_billing_usage` using MERGE on `record_id` for idempotency. Tracks the high-water mark in `finops.meta.pipeline_watermarks`.

# COMMAND ----------

# DBTITLE 1,Incremental MERGE from system.billing.usage
import uuid
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

batch_id  = str(uuid.uuid4())
pipeline  = "ingest_billing_usage"
tbl       = "bronze_billing_usage"
start_ts  = datetime.now()

# ------------------------------------------------------------------
# 1. Read watermark for incremental processing
# ------------------------------------------------------------------
wm = spark.sql(f"""
    SELECT COALESCE(MAX(last_watermark), TIMESTAMP('2020-01-01')) AS wm
    FROM finops.meta.pipeline_watermarks
    WHERE pipeline_name = '{pipeline}' AND table_name = '{tbl}'
""").collect()[0]['wm']
print(f"Watermark: {wm}")

# ------------------------------------------------------------------
# 2. Read incremental source from system.billing.usage
# ------------------------------------------------------------------
src = spark.sql(f"""
    SELECT account_id, workspace_id, record_id, sku_name, cloud,
           usage_start_time, usage_end_time, usage_date,
           custom_tags, usage_unit, usage_quantity,
           usage_metadata, identity_metadata, record_type,
           ingestion_date, billing_origin_product, product_features, usage_type
      FROM system.billing.usage
     WHERE usage_start_time > TIMESTAMP('{wm}')
""")

cnt = src.count()
print(f"Records to process: {cnt}")

if cnt > 0:
    # 3. Add ingestion lineage columns
    src = (src
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_date",   current_timestamp().cast('date'))
        .withColumn("_source_system",    lit("system.billing.usage"))
        .withColumn("_batch_id",         lit(batch_id))
    )
    src.createOrReplaceTempView("vw_new_billing_usage")

    # 4. MERGE into bronze (idempotent upsert on record_id)
    metrics = spark.sql("""
        MERGE INTO finops.bronze.bronze_billing_usage AS t
        USING vw_new_billing_usage AS s
        ON t.record_id = s.record_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
    print(f"Inserted: {ins}  Updated: {upd}")

    # 5. Advance watermark
    spark.sql(f"""
        MERGE INTO finops.meta.pipeline_watermarks AS t
        USING (SELECT '{pipeline}' AS pipeline_name, '{tbl}' AS table_name,
                      'usage_start_time' AS watermark_column,
                      MAX(usage_start_time) AS last_watermark,
                      CURRENT_DATE AS last_run_date,
                      CURRENT_TIMESTAMP AS updated_at
                 FROM vw_new_billing_usage) AS s
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
    VALUES ('{batch_id}', '{pipeline}', '02_ingest_billing_usage',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")