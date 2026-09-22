# Databricks notebook source
# DBTITLE 1,03 — Ingest Billing Prices
# MAGIC %md
# MAGIC # 03 — Ingest Billing Prices (Bronze)
# MAGIC
# MAGIC Full-refresh loads `system.billing.list_prices` into `finops.bronze.bronze_billing_prices`. Prices change infrequently so a full MERGE on natural key is used.

# COMMAND ----------

# DBTITLE 1,Full-refresh MERGE from system.billing.list_prices
import uuid
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

batch_id  = str(uuid.uuid4())
pipeline  = "ingest_billing_prices"
tbl       = "bronze_billing_prices"
start_ts  = datetime.now()

# ------------------------------------------------------------------
# 1. Full refresh — prices change infrequently
# ------------------------------------------------------------------
src = spark.sql("""
    SELECT account_id, price_start_time, price_end_time,
           sku_name, cloud, currency_code, usage_unit, pricing,
           'list_prices' AS price_source
      FROM system.billing.list_prices
""")
cnt = src.count()
print(f"Price records to load: {cnt}")

# 2. Add ingestion lineage
src = (src
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_ingestion_date",   current_timestamp().cast('date'))
    .withColumn("_source_system",    lit("system.billing.list_prices"))
    .withColumn("_batch_id",         lit(batch_id))
)
src.createOrReplaceTempView("vw_new_prices")

# 3. MERGE into bronze on natural key (sku_name, cloud, price_start_time)
metrics = spark.sql("""
    MERGE INTO finops.bronze.bronze_billing_prices AS t
    USING vw_new_prices AS s
    ON  t.sku_name = s.sku_name
    AND t.cloud    = s.cloud
    AND t.price_start_time = s.price_start_time
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").collect()[0]
ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
print(f"Inserted: {ins}  Updated: {upd}")

# 4. Log execution
end_ts = datetime.now()
dur   = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '03_ingest_billing_prices',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")