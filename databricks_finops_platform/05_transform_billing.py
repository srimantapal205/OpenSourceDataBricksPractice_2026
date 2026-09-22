# Databricks notebook source
# DBTITLE 1,05 — Transform Billing
# MAGIC %md
# MAGIC # 05 — Transform Billing (Bronze → Silver)
# MAGIC
# MAGIC Cleanses `bronze_billing_usage`, extracts metadata fields from nested structs, enriches with `dim_workspace`, and MERGES into `silver_billing_usage` on `record_id`.

# COMMAND ----------

# DBTITLE 1,Bronze → Silver billing transformation
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "transform_billing"
start_ts = datetime.now()

# ------------------------------------------------------------------
# Transform bronze → silver: extract metadata, enrich with workspace
# ------------------------------------------------------------------
src = spark.sql("""
    SELECT
        b.usage_date, b.usage_start_time, b.usage_end_time,
        b.workspace_id,
        w.workspace_name,
        w.environment,
        b.cloud,
        w.region,
        b.sku_name  AS sku,
        b.usage_unit,
        b.usage_quantity,
        b.record_type,
        b.billing_origin_product,
        b.usage_metadata.cluster_id,
        b.usage_metadata.job_id,
        b.usage_metadata.warehouse_id,
        b.usage_metadata.instance_pool_id,
        b.usage_metadata.node_type,
        b.usage_metadata.job_name,
        b.usage_metadata.job_run_id,
        b.usage_metadata.notebook_id,
        b.usage_metadata.notebook_path,
        b.usage_metadata.dlt_pipeline_id,
        b.usage_metadata.app_name,
        b.identity_metadata.run_by   AS user_id,
        b.identity_metadata.run_as   AS service_principal_id,
        b.custom_tags,
        STRUCT(
            b.product_features.is_serverless,
            b.product_features.is_photon,
            b.product_features.serving_type,
            b.product_features.jobs_tier,
            b.product_features.sql_tier,
            b.product_features.dlt_tier
        ) AS product_features,
        b.usage_type,
        b.record_id,
        current_timestamp() AS ingestion_timestamp
    FROM finops.bronze.bronze_billing_usage b
    LEFT JOIN finops.dims.dim_workspace w ON b.workspace_id = w.workspace_id
""")
cnt = src.count()
print(f"Records to transform: {cnt}")
src.createOrReplaceTempView("vw_silver_billing")

metrics = spark.sql("""
    MERGE INTO finops.silver.silver_billing_usage AS t
    USING vw_silver_billing AS s
    ON t.record_id = s.record_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").collect()[0]
ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
print(f"Inserted: {ins}  Updated: {upd}")

# Update dim_sku from billing data
spark.sql("""
    MERGE INTO finops.dims.dim_sku AS t
    USING (
        SELECT DISTINCT
            sku_name AS sku,
            billing_origin_product,
            cloud,
            usage_unit,
            billing_origin_product AS product
        FROM finops.bronze.bronze_billing_usage
        WHERE sku_name IS NOT NULL
    ) AS s
    ON t.sku = s.sku
    WHEN NOT MATCHED THEN INSERT *
""")

# Update dim_user from identity metadata
spark.sql("""
    MERGE INTO finops.dims.dim_user AS t
    USING (
        SELECT DISTINCT
            identity_metadata.run_by AS user_id,
            NULL AS user_name,
            identity_metadata.run_by AS email,
            'user' AS user_type,
            NULL AS department,
            NULL AS business_unit,
            'Active' AS status
        FROM finops.bronze.bronze_billing_usage
        WHERE identity_metadata.run_by IS NOT NULL
        UNION
        SELECT DISTINCT
            identity_metadata.run_as AS user_id,
            NULL AS user_name,
            identity_metadata.run_as AS email,
            'servicePrincipal' AS user_type,
            NULL AS department,
            NULL AS business_unit,
            'Active' AS status
        FROM finops.bronze.bronze_billing_usage
        WHERE identity_metadata.run_as IS NOT NULL
    ) AS s
    ON t.user_id = s.user_id
    WHEN NOT MATCHED THEN INSERT *
""")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '05_transform_billing',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")