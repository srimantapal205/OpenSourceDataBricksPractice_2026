# Databricks notebook source
# DBTITLE 1,07 — Transform Audit
# MAGIC %md
# MAGIC # 07 — Transform Audit (Bronze → Fact)
# MAGIC
# MAGIC Extracts user identity, response status, and action details from `bronze_audit_events`, enriches with `dim_workspace`, and MERGES into `fact_audit_event` on `audit_event_id`.

# COMMAND ----------

# DBTITLE 1,Bronze → Fact audit transformation
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "transform_audit"
start_ts = datetime.now()

# ------------------------------------------------------------------
# Transform bronze audit → fact_audit_event
# ------------------------------------------------------------------
src = spark.sql("""
    SELECT
        b.event_id AS audit_event_id,
        b.event_time,
        b.event_date,
        b.workspace_id,
        w.workspace_name,
        COALESCE(b.user_identity.email, b.user_identity.subject_name,
                 b.identity_metadata.run_by, b.identity_metadata.run_as) AS user_name,
        CASE
            WHEN b.identity_metadata.run_as IS NOT NULL
             AND b.identity_metadata.run_as != b.identity_metadata.run_by
            THEN 'servicePrincipal'
            ELSE 'user'
        END AS user_type,
        b.identity_metadata.run_as AS service_principal,
        b.action_name,
        b.service_name,
        b.request_id,
        b.source_ip_address AS source_ip,
        b.user_agent,
        NULL AS object_type,
        NULL AS object_id,
        NULL AS object_name,
        b.response.status_code  AS response_status,
        b.response.error_message AS response_error,
        b.audit_level,
        b.request_params AS request_parameters,
        to_json(b.request_params) AS audit_details,
        current_timestamp() AS ingestion_timestamp
    FROM finops.bronze.bronze_audit_events b
    LEFT JOIN finops.dims.dim_workspace w ON b.workspace_id = w.workspace_id
""")
cnt = src.count()
print(f"Records to transform: {cnt}")
src.createOrReplaceTempView("vw_fact_audit")

metrics = spark.sql("""
    MERGE INTO finops.silver.fact_audit_event AS t
    USING vw_fact_audit AS s
    ON t.audit_event_id = s.audit_event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").collect()[0]
ins, upd = metrics['num_inserted_rows'], metrics['num_updated_rows']
print(f"Inserted: {ins}  Updated: {upd}")

# Update dim_user from audit identity data
spark.sql("""
    MERGE INTO finops.dims.dim_user AS t
    USING (
        SELECT DISTINCT
            user_identity.email AS user_id,
            user_identity.subject_name AS user_name,
            user_identity.email AS email,
            'user' AS user_type,
            NULL AS department,
            NULL AS business_unit,
            'Active' AS status
        FROM finops.bronze.bronze_audit_events
        WHERE user_identity.email IS NOT NULL
    ) AS s
    ON t.user_id = s.user_id
    WHEN NOT MATCHED THEN INSERT *
""")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '07_transform_audit',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            {cnt}, {ins}, {upd}, 0, 'SUCCESS', NULL)
""")
print(f"Done. Records={cnt}  Inserted={ins}  Updated={upd}  Duration={dur:.1f}s")