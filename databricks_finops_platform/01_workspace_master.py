# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Databricks FinOps Platform — Notebook 01: Unity Catalog Setup & DDL
# MAGIC
# MAGIC This notebook creates the complete Unity Catalog structure for the Multi-Workspace Audit, Usage, Cost & Billing Monitoring Platform. It defines all catalogs, schemas, dimension tables, fact tables, bronze/silver/gold tables, and monitoring tables.

# COMMAND ----------

# DBTITLE 1,Create Catalog & Schemas
# MAGIC %sql
# MAGIC -- Create the finops catalog (skip if exists)
# MAGIC CREATE CATALOG IF NOT EXISTS finops
# MAGIC   COMMENT 'Centralized Databricks FinOps & Audit Monitoring Platform';
# MAGIC
# MAGIC -- Create schemas for medallion architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS finops.bronze
# MAGIC   COMMENT 'Raw append-oriented ingestion layer preserving source system records';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS finops.silver
# MAGIC   COMMENT 'Cleansed, standardized, and enriched data layer';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS finops.gold
# MAGIC   COMMENT 'Optimized aggregation tables for dashboard consumption';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS finops.dims
# MAGIC   COMMENT 'Reusable dimension tables';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS finops.meta
# MAGIC   COMMENT 'Framework monitoring and data quality logs';

# COMMAND ----------

# DBTITLE 1,Create dim_workspace
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finops.dims.dim_workspace (
# MAGIC   workspace_id           STRING        NOT NULL COMMENT 'Unique Databricks workspace ID',
# MAGIC   workspace_name         STRING        COMMENT 'Workspace display name (resolved from account metadata)',
# MAGIC   workspace_url          STRING        COMMENT 'Workspace URL',
# MAGIC   account_id             STRING        COMMENT 'Databricks account ID',
# MAGIC   cloud                  STRING        COMMENT 'AWS, AZURE, or GCP',
# MAGIC   region                 STRING        COMMENT 'Workspace deployment region',
# MAGIC   environment            STRING        COMMENT 'DEV, TEST, UAT, or PROD',
# MAGIC   business_unit          STRING        COMMENT 'Owning business unit',
# MAGIC   department             STRING        COMMENT 'Owning department',
# MAGIC   cost_center            STRING        COMMENT 'Cost center code',
# MAGIC   owner                  STRING        COMMENT 'Workspace owner email or name',
# MAGIC   status                 STRING        DEFAULT 'Active' COMMENT 'Active or Inactive',
# MAGIC   created_date           DATE          COMMENT 'Workspace creation date',
# MAGIC   last_updated_timestamp TIMESTAMP     COMMENT 'Last metadata update'
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id)
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC COMMENT 'Centralized workspace master dimension — single source of truth for workspace metadata';

# COMMAND ----------

# DBTITLE 1,Create dim_date
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finops.dims.dim_date (
# MAGIC   date          DATE     NOT NULL COMMENT 'Calendar date',
# MAGIC   day           INT      COMMENT 'Day of month (1-31)',
# MAGIC   day_of_week   INT      COMMENT 'Day of week (1=Sunday, 7=Saturday)',
# MAGIC   day_name      STRING   COMMENT 'Day name (Monday, Tuesday, ...)',
# MAGIC   week_of_year  INT      COMMENT 'ISO week number (1-53)',
# MAGIC   month         INT      COMMENT 'Month number (1-12)',
# MAGIC   month_name    STRING   COMMENT 'Month name (January, February, ...)',
# MAGIC   quarter       INT      COMMENT 'Quarter (1-4)',
# MAGIC   year          INT      COMMENT 'Calendar year',
# MAGIC   fiscal_year   INT      COMMENT 'Fiscal year',
# MAGIC   fiscal_month  INT      COMMENT 'Fiscal month number',
# MAGIC   is_weekend    BOOLEAN  COMMENT 'True if Saturday or Sunday',
# MAGIC   is_month_start BOOLEAN COMMENT 'True if first day of month',
# MAGIC   is_month_end  BOOLEAN  COMMENT 'True if last day of month'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Standard calendar dimension with fiscal period support';

# COMMAND ----------

# DBTITLE 1,Create dim_environment, dim_user, dim_sku
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finops.dims.dim_environment (
# MAGIC   environment  STRING  NOT NULL,
# MAGIC   description  STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Environment dimension (DEV, TEST, UAT, PROD)';
# MAGIC
# MAGIC -- Seed environment dimension
# MAGIC MERGE INTO finops.dims.dim_environment AS t
# MAGIC USING (
# MAGIC   SELECT 'DEV' AS environment, 'Development environment for experimentation and testing' AS description
# MAGIC   UNION ALL
# MAGIC   SELECT 'TEST', 'Quality assurance and integration testing'
# MAGIC   UNION ALL
# MAGIC   SELECT 'UAT', 'User acceptance testing'
# MAGIC   UNION ALL
# MAGIC   SELECT 'PROD', 'Production environment'
# MAGIC ) AS s
# MAGIC ON t.environment = s.environment
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS finops.dims.dim_user (
# MAGIC   user_id        STRING  NOT NULL COMMENT 'Unique user identifier (email or subject name)',
# MAGIC   user_name      STRING  COMMENT 'Display name',
# MAGIC   email          STRING  COMMENT 'Email address',
# MAGIC   user_type      STRING  COMMENT 'user or servicePrincipal',
# MAGIC   department     STRING  COMMENT 'Department',
# MAGIC   business_unit  STRING  COMMENT 'Business unit',
# MAGIC   status         STRING  DEFAULT 'Active'
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (user_id)
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC COMMENT 'User dimension derived from audit logs and billing identity metadata';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS finops.dims.dim_sku (
# MAGIC   sku                    STRING  NOT NULL,
# MAGIC   product                STRING  COMMENT 'Product category derived from SKU name',
# MAGIC   billing_origin_product STRING  COMMENT 'Billing origin product from system.billing.usage',
# MAGIC   cloud                  STRING  COMMENT 'Cloud (AWS, AZURE, GCP)',
# MAGIC   usage_unit             STRING  COMMENT 'Unit of measurement (e.g., DBUs)',
# MAGIC   description            STRING  COMMENT 'Human-readable SKU description'
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (sku)
# MAGIC COMMENT 'SKU dimension with product categorization';

# COMMAND ----------

# DBTITLE 1,Create Bronze Tables
# MAGIC %sql
# MAGIC -- Bronze Billing Usage
# MAGIC CREATE TABLE IF NOT EXISTS finops.bronze.bronze_billing_usage (
# MAGIC   account_id            STRING,
# MAGIC   workspace_id          STRING,
# MAGIC   record_id             STRING,
# MAGIC   sku_name              STRING,
# MAGIC   cloud                 STRING,
# MAGIC   usage_start_time      TIMESTAMP,
# MAGIC   usage_end_time        TIMESTAMP,
# MAGIC   usage_date            DATE,
# MAGIC   custom_tags           MAP<STRING, STRING>,
# MAGIC   usage_unit            STRING,
# MAGIC   usage_quantity        DECIMAL(38,18),
# MAGIC   usage_metadata        STRUCT<
# MAGIC     cluster_id:STRING, job_id:STRING, warehouse_id:STRING, instance_pool_id:STRING,
# MAGIC     node_type:STRING, job_run_id:STRING, notebook_id:STRING, dlt_pipeline_id:STRING,
# MAGIC     endpoint_name:STRING, endpoint_id:STRING, dlt_update_id:STRING, dlt_maintenance_id:STRING,
# MAGIC     run_name:STRING, job_name:STRING, notebook_path:STRING, central_clean_room_id:STRING,
# MAGIC     source_region:STRING, destination_region:STRING, app_id:STRING, app_name:STRING,
# MAGIC     metastore_id:STRING, private_endpoint_name:STRING, storage_api_type:STRING,
# MAGIC     budget_policy_id:STRING, ai_runtime_pool_id:STRING, ai_runtime_workload_id:STRING,
# MAGIC     uc_table_catalog:STRING, uc_table_schema:STRING, uc_table_name:STRING,
# MAGIC     database_instance_id:STRING, sharing_materialization_id:STRING, schema_id:STRING,
# MAGIC     usage_policy_id:STRING, base_environment_id:STRING, agent_bricks_id:STRING,
# MAGIC     index_id:STRING, catalog_id:STRING, project_id:STRING, branch_id:STRING,
# MAGIC     table_id:STRING, supervisor_agent_id:STRING, networking_client:STRING,
# MAGIC     associated_product:STRING, recipient_id:STRING, interactive_source:STRING,
# MAGIC     snapshot_id:STRING, ai_gateway:STRUCT<endpoint_name:STRING, endpoint_id:STRING,
# MAGIC     destination_id:STRING, destination_model:STRING>,
# MAGIC     genie:STRUCT<surface:STRING, channel:STRING, agent_id:STRING>,
# MAGIC     data_source:STRING, operation:STRING, serverless_compute_id:STRING,
# MAGIC     entity_full_name:STRING, failover_group_name:STRING
# MAGIC   >,
# MAGIC   identity_metadata     STRUCT<run_as:STRING, created_by:STRING, owned_by:STRING, run_by:STRING>,
# MAGIC   record_type           STRING,
# MAGIC   ingestion_date        DATE,
# MAGIC   billing_origin_product STRING,
# MAGIC   product_features      STRUCT<
# MAGIC     jobs_tier:STRING, sql_tier:STRING, dlt_tier:STRING, is_serverless:BOOLEAN,
# MAGIC     is_photon:BOOLEAN, serving_type:STRING,
# MAGIC     networking:STRUCT<connectivity_type:STRING>,
# MAGIC     ai_runtime:STRUCT<compute_type:STRING>,
# MAGIC     model_serving:STRUCT<offering_type:STRING, reservation_term:STRING>,
# MAGIC     ai_gateway:STRUCT<feature_type:STRING>,
# MAGIC     serverless_gpu:STRUCT<workload_type:STRING>,
# MAGIC     agent_bricks:STRUCT<problem_type:STRING, workload_type:STRING>,
# MAGIC     performance_target:STRING,
# MAGIC     ai_functions:STRUCT<ai_function:STRING>,
# MAGIC     apps:STRUCT<compute_size:STRING>,
# MAGIC     lakeflow_connect:STRUCT<task_type:STRING, zerobus_request_type:STRING>,
# MAGIC     lakebase:STRUCT<storage_type:STRING, compute_type:STRING>,
# MAGIC     ai_bi_genie:STRUCT<capability_type:STRING, offering_type:STRING>,
# MAGIC     genie:STRUCT<offering_type:STRING>
# MAGIC   >,
# MAGIC   usage_type             STRING,
# MAGIC   -- Ingestion metadata
# MAGIC   _ingestion_timestamp  TIMESTAMP,
# MAGIC   _ingestion_date       DATE,
# MAGIC   _source_system        STRING,
# MAGIC   _batch_id             STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, usage_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 30 days'
# MAGIC )
# MAGIC COMMENT 'Raw billing usage records from system.billing.usage with ingestion lineage';
# MAGIC
# MAGIC -- Bronze Billing Prices
# MAGIC CREATE TABLE IF NOT EXISTS finops.bronze.bronze_billing_prices (
# MAGIC   account_id        STRING,
# MAGIC   price_start_time  TIMESTAMP,
# MAGIC   price_end_time    TIMESTAMP,
# MAGIC   sku_name          STRING,
# MAGIC   cloud             STRING,
# MAGIC   currency_code     STRING,
# MAGIC   usage_unit        STRING,
# MAGIC   pricing           STRUCT<
# MAGIC     default:DECIMAL(38,18),
# MAGIC     promotional:STRUCT<default:DECIMAL(38,18)>,
# MAGIC     effective_list:STRUCT<default:DECIMAL(38,18)>
# MAGIC   >,
# MAGIC   price_source      STRING COMMENT 'list_prices or account_prices',
# MAGIC   _ingestion_timestamp TIMESTAMP,
# MAGIC   _ingestion_date   DATE,
# MAGIC   _source_system    STRING,
# MAGIC   _batch_id         STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (_ingestion_date)
# MAGIC COMMENT 'Raw SKU pricing records from system.billing.list_prices and account_prices';
# MAGIC
# MAGIC -- Bronze Audit Events
# MAGIC CREATE TABLE IF NOT EXISTS finops.bronze.bronze_audit_events (
# MAGIC   account_id          STRING,
# MAGIC   workspace_id        STRING,
# MAGIC   version             STRING,
# MAGIC   event_time          TIMESTAMP,
# MAGIC   event_date          DATE,
# MAGIC   source_ip_address   STRING,
# MAGIC   user_agent          STRING,
# MAGIC   session_id          STRING,
# MAGIC   user_identity       STRUCT<email:STRING, subject_name:STRING>,
# MAGIC   service_name        STRING,
# MAGIC   action_name         STRING,
# MAGIC   request_id          STRING,
# MAGIC   request_params      MAP<STRING, STRING>,
# MAGIC   response            STRUCT<status_code:INT, error_message:STRING, result:STRING>,
# MAGIC   audit_level         STRING,
# MAGIC   event_id            STRING,
# MAGIC   identity_metadata   STRUCT<
# MAGIC     run_by:STRING, run_as:STRING, acting_resource:STRING,
# MAGIC     run_by_display_name:STRING, run_as_display_name:STRING
# MAGIC   >,
# MAGIC   _ingestion_timestamp TIMESTAMP,
# MAGIC   _ingestion_date      DATE,
# MAGIC   _source_system       STRING,
# MAGIC   _batch_id            STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, event_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 30 days'
# MAGIC )
# MAGIC COMMENT 'Raw audit events from system.access.audit with ingestion lineage';
# MAGIC
# MAGIC -- Bronze Workspace Master
# MAGIC CREATE TABLE IF NOT EXISTS finops.bronze.bronze_workspace_master (
# MAGIC   workspace_id          STRING,
# MAGIC   workspace_name        STRING,
# MAGIC   workspace_url         STRING,
# MAGIC   account_id            STRING,
# MAGIC   cloud                 STRING,
# MAGIC   region                STRING,
# MAGIC   environment           STRING,
# MAGIC   business_unit         STRING,
# MAGIC   department            STRING,
# MAGIC   cost_center           STRING,
# MAGIC   owner                 STRING,
# MAGIC   status                STRING,
# MAGIC   created_date          DATE,
# MAGIC   last_updated_timestamp TIMESTAMP,
# MAGIC   _ingestion_timestamp  TIMESTAMP,
# MAGIC   _ingestion_date       DATE,
# MAGIC   _source_system        STRING,
# MAGIC   _batch_id             STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (_ingestion_date)
# MAGIC COMMENT 'Raw workspace master records';

# COMMAND ----------

# DBTITLE 1,Create Silver & Fact Tables
# MAGIC %sql
# MAGIC -- Silver Billing Usage
# MAGIC CREATE TABLE IF NOT EXISTS finops.silver.silver_billing_usage (
# MAGIC   usage_date             DATE,
# MAGIC   usage_start_time       TIMESTAMP,
# MAGIC   usage_end_time         TIMESTAMP,
# MAGIC   workspace_id           STRING,
# MAGIC   workspace_name         STRING,
# MAGIC   environment            STRING,
# MAGIC   cloud                  STRING,
# MAGIC   region                 STRING,
# MAGIC   sku                    STRING,
# MAGIC   usage_unit             STRING,
# MAGIC   usage_quantity         DECIMAL(38,18),
# MAGIC   record_type            STRING,
# MAGIC   billing_origin_product STRING,
# MAGIC   job_id                 STRING,
# MAGIC   cluster_id             STRING,
# MAGIC   warehouse_id           STRING,
# MAGIC   instance_pool_id      STRING,
# MAGIC   node_type              STRING,
# MAGIC   job_name               STRING,
# MAGIC   job_run_id             STRING,
# MAGIC   notebook_id            STRING,
# MAGIC   notebook_path          STRING,
# MAGIC   dlt_pipeline_id        STRING,
# MAGIC   app_name               STRING,
# MAGIC   user_id                STRING,
# MAGIC   service_principal_id   STRING,
# MAGIC   custom_tags            MAP<STRING, STRING>,
# MAGIC   product_features       STRUCT<
# MAGIC     is_serverless:BOOLEAN, is_photon:BOOLEAN, serving_type:STRING,
# MAGIC     jobs_tier:STRING, sql_tier:STRING, dlt_tier:STRING
# MAGIC   >,
# MAGIC   usage_type             STRING,
# MAGIC   record_id              STRING,
# MAGIC   ingestion_timestamp    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, sku)
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
# MAGIC COMMENT 'Cleansed and standardized billing usage with workspace enrichment and metadata extraction';
# MAGIC
# MAGIC -- Fact: Databricks Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.silver.fact_databricks_cost (
# MAGIC   cost_id               STRING  NOT NULL COMMENT 'Surrogate key = record_id',
# MAGIC   usage_date            DATE,
# MAGIC   usage_start_time      TIMESTAMP,
# MAGIC   usage_end_time        TIMESTAMP,
# MAGIC   workspace_id          STRING,
# MAGIC   workspace_name        STRING,
# MAGIC   sku                   STRING,
# MAGIC   usage_unit            STRING,
# MAGIC   usage_quantity        DECIMAL(38,18),
# MAGIC   unit_price            DECIMAL(38,18) COMMENT 'Effective price from list_prices or account_prices',
# MAGIC   currency             STRING,
# MAGIC   estimated_cost        DECIMAL(38,18) GENERATED ALWAYS AS (CAST(usage_quantity * unit_price AS DECIMAL(38,18))),
# MAGIC   record_type           STRING,
# MAGIC   billing_origin_product STRING,
# MAGIC   job_id                STRING,
# MAGIC   cluster_id            STRING,
# MAGIC   warehouse_id          STRING,
# MAGIC   user_id               STRING,
# MAGIC   service_principal_id  STRING,
# MAGIC   cost_center           STRING,
# MAGIC   environment           STRING,
# MAGIC   business_unit         STRING,
# MAGIC   region                STRING,
# MAGIC   cloud                 STRING,
# MAGIC   ingestion_timestamp   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, sku)
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
# MAGIC COMMENT 'Normalized billing fact with estimated cost = usage_quantity * applicable_unit_price';
# MAGIC
# MAGIC -- Fact: Audit Event
# MAGIC CREATE TABLE IF NOT EXISTS finops.silver.fact_audit_event (
# MAGIC   audit_event_id    STRING NOT NULL COMMENT 'Surrogate key = event_id',
# MAGIC   event_time        TIMESTAMP,
# MAGIC   event_date        DATE,
# MAGIC   workspace_id      STRING,
# MAGIC   workspace_name    STRING,
# MAGIC   user_name         STRING,
# MAGIC   user_type         STRING COMMENT 'user or servicePrincipal',
# MAGIC   service_principal STRING,
# MAGIC   action_name       STRING,
# MAGIC   service_name      STRING,
# MAGIC   request_id        STRING,
# MAGIC   source_ip         STRING,
# MAGIC   user_agent        STRING,
# MAGIC   object_type       STRING,
# MAGIC   object_id         STRING,
# MAGIC   object_name       STRING,
# MAGIC   response_status   INT    COMMENT 'HTTP status code from response',
# MAGIC   response_error    STRING,
# MAGIC   audit_level       STRING,
# MAGIC   request_parameters MAP<STRING, STRING>,
# MAGIC   audit_details     STRING  COMMENT 'JSON serialization of full raw audit payload',
# MAGIC   ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, user_name)
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
# MAGIC COMMENT 'Normalized audit events with extracted fields and preserved raw payload';
# MAGIC
# MAGIC -- Fact: Cost Allocation
# MAGIC CREATE TABLE IF NOT EXISTS finops.silver.fact_cost_allocation (
# MAGIC   allocation_id     STRING NOT NULL COMMENT 'UUID surrogate key',
# MAGIC   allocation_date   DATE,
# MAGIC   workspace_id      STRING,
# MAGIC   workspace_name    STRING,
# MAGIC   cost_center       STRING,
# MAGIC   business_unit    STRING,
# MAGIC   department       STRING,
# MAGIC   project          STRING,
# MAGIC   application      STRING,
# MAGIC   environment      STRING,
# MAGIC   cloud            STRING,
# MAGIC   sku              STRING,
# MAGIC   usage_quantity   DECIMAL(38,18),
# MAGIC   estimated_cost   DECIMAL(38,18),
# MAGIC   job_id           STRING,
# MAGIC   cluster_id       STRING,
# MAGIC   warehouse_id     STRING,
# MAGIC   user_id          STRING,
# MAGIC   tag_source       STRING COMMENT 'workspace_metadata or custom_tags',
# MAGIC   ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, cost_center)
# MAGIC COMMENT 'Normalized cost allocation model using workspace metadata and custom tags';
# MAGIC
# MAGIC -- Fact: Cost Anomaly
# MAGIC CREATE TABLE IF NOT EXISTS finops.silver.fact_cost_anomaly (
# MAGIC   anomaly_id            STRING NOT NULL COMMENT 'UUID surrogate key',
# MAGIC   detection_date        DATE,
# MAGIC   workspace_id          STRING,
# MAGIC   workspace_name        STRING,
# MAGIC   anomaly_type          STRING COMMENT 'daily_cost_spike, sku_spike, dbu_spike, weekend_usage, after_hours_usage, user_activity, warehouse_cost, job_cost, cluster_cost',
# MAGIC   metric                STRING COMMENT 'Name of the metric being evaluated',
# MAGIC   baseline_value        DECIMAL(38,18),
# MAGIC   actual_value          DECIMAL(38,18),
# MAGIC   deviation_percentage  DECIMAL(10,2),
# MAGIC   severity              STRING COMMENT 'Critical, High, Medium, Low',
# MAGIC   detected_timestamp    TIMESTAMP,
# MAGIC   status                STRING DEFAULT 'Open' COMMENT 'Open, Acknowledged, Resolved'
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (workspace_id, severity)
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC COMMENT 'Detected cost anomalies with baseline comparison and severity classification';

# COMMAND ----------

# DBTITLE 1,Create Gold Tables
# MAGIC %sql
# MAGIC -- Gold: Workspace Daily Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_workspace_daily_cost (
# MAGIC   usage_date              DATE,
# MAGIC   workspace_id            STRING,
# MAGIC   workspace_name          STRING,
# MAGIC   environment             STRING,
# MAGIC   total_usage             DECIMAL(38,18),
# MAGIC   total_cost              DECIMAL(38,18),
# MAGIC   previous_day_cost       DECIMAL(38,18),
# MAGIC   cost_change             DECIMAL(38,18),
# MAGIC   cost_change_percentage  DECIMAL(10,2)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, usage_date)
# MAGIC COMMENT 'Daily cost summary per workspace with day-over-day change';
# MAGIC
# MAGIC -- Gold: Workspace Monthly Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_workspace_monthly_cost (
# MAGIC   month                      STRING COMMENT 'YYYY-MM',
# MAGIC   workspace_id               STRING,
# MAGIC   workspace_name             STRING,
# MAGIC   total_usage                DECIMAL(38,18),
# MAGIC   total_cost                 DECIMAL(38,18),
# MAGIC   previous_month_cost        DECIMAL(38,18),
# MAGIC   month_over_month_percentage DECIMAL(10,2)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, month)
# MAGIC COMMENT 'Monthly cost summary per workspace with MoM percentage';
# MAGIC
# MAGIC -- Gold: SKU Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_sku_cost (
# MAGIC   usage_date      DATE,
# MAGIC   workspace_id    STRING,
# MAGIC   sku             STRING,
# MAGIC   usage_quantity  DECIMAL(38,18),
# MAGIC   estimated_cost  DECIMAL(38,18)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, sku, usage_date)
# MAGIC COMMENT 'Daily SKU-level cost breakdown by workspace';
# MAGIC
# MAGIC -- Gold: User Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_user_cost (
# MAGIC   usage_date      DATE,
# MAGIC   workspace_id    STRING,
# MAGIC   user_name       STRING,
# MAGIC   usage_quantity  DECIMAL(38,18),
# MAGIC   estimated_cost  DECIMAL(38,18)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, user_name, usage_date)
# MAGIC COMMENT 'Daily user-level cost breakdown by workspace';
# MAGIC
# MAGIC -- Gold: Job Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_job_cost (
# MAGIC   usage_date      DATE,
# MAGIC   workspace_id    STRING,
# MAGIC   job_id          STRING,
# MAGIC   job_name        STRING,
# MAGIC   usage_quantity  DECIMAL(38,18),
# MAGIC   estimated_cost  DECIMAL(38,18)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, job_id, usage_date)
# MAGIC COMMENT 'Daily job-level cost breakdown by workspace';
# MAGIC
# MAGIC -- Gold: Cluster Cost
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_cluster_cost (
# MAGIC   usage_date      DATE,
# MAGIC   workspace_id    STRING,
# MAGIC   cluster_id      STRING,
# MAGIC   cluster_name    STRING,
# MAGIC   usage_quantity  DECIMAL(38,18),
# MAGIC   estimated_cost  DECIMAL(38,18)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, cluster_id, usage_date)
# MAGIC COMMENT 'Daily cluster-level cost breakdown by workspace';
# MAGIC
# MAGIC -- Gold: Audit Summary
# MAGIC CREATE TABLE IF NOT EXISTS finops.gold.gold_audit_summary (
# MAGIC   event_date         DATE,
# MAGIC   workspace_id       STRING,
# MAGIC   action_name        STRING,
# MAGIC   user_name          STRING,
# MAGIC   event_count        BIGINT,
# MAGIC   failed_event_count BIGINT
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (workspace_id, event_date)
# MAGIC COMMENT 'Daily audit event summary by workspace, action, and user';

# COMMAND ----------

# DBTITLE 1,Create Monitoring Tables
# MAGIC %sql
# MAGIC -- Data Quality Execution Log
# MAGIC CREATE TABLE IF NOT EXISTS finops.meta.dq_execution_log (
# MAGIC   dq_run_id            STRING NOT NULL,
# MAGIC   execution_timestamp  TIMESTAMP,
# MAGIC   table_name           STRING,
# MAGIC   check_name           STRING,
# MAGIC   total_records        BIGINT,
# MAGIC   failed_records       BIGINT,
# MAGIC   status               STRING COMMENT 'PASS, WARN, FAIL',
# MAGIC   error_message        STRING
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (execution_timestamp)
# MAGIC COMMENT 'Data quality check execution results';
# MAGIC
# MAGIC -- Framework Execution Log
# MAGIC CREATE TABLE IF NOT EXISTS finops.meta.framework_execution_log (
# MAGIC   run_id              STRING NOT NULL,
# MAGIC   pipeline_name       STRING,
# MAGIC   notebook_name       STRING,
# MAGIC   start_time          TIMESTAMP,
# MAGIC   end_time            TIMESTAMP,
# MAGIC   duration_seconds    DOUBLE,
# MAGIC   records_read        BIGINT,
# MAGIC   records_inserted    BIGINT,
# MAGIC   records_updated     BIGINT,
# MAGIC   records_rejected    BIGINT,
# MAGIC   status              STRING COMMENT 'RUNNING, SUCCESS, FAILED',
# MAGIC   error_message       STRING
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (start_time)
# MAGIC COMMENT 'Pipeline execution health monitoring log';
# MAGIC
# MAGIC -- Workspace Control Table (for incremental watermarks)
# MAGIC CREATE TABLE IF NOT EXISTS finops.meta.pipeline_watermarks (
# MAGIC   pipeline_name    STRING NOT NULL,
# MAGIC   table_name       STRING NOT NULL,
# MAGIC   watermark_column STRING NOT NULL,
# MAGIC   last_watermark   TIMESTAMP,
# MAGIC   last_run_date    DATE,
# MAGIC   updated_at       TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (pipeline_name, table_name)
# MAGIC COMMENT 'Incremental processing watermark tracking for idempotent pipelines';

# COMMAND ----------

# DBTITLE 1,Populate dim_date
# MAGIC %sql
# MAGIC -- Generate dim_date for 2020-01-01 through 2030-12-31
# MAGIC MERGE INTO finops.dims.dim_date AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     date AS date,
# MAGIC     DAY(date) AS day,
# MAGIC     DAYOFWEEK(date) AS day_of_week,
# MAGIC     DATE_FORMAT(date, 'EEEE') AS day_name,
# MAGIC     WEEKOFYEAR(date) AS week_of_year,
# MAGIC     MONTH(date) AS month,
# MAGIC     DATE_FORMAT(date, 'MMMM') AS month_name,
# MAGIC     QUARTER(date) AS quarter,
# MAGIC     YEAR(date) AS year,
# MAGIC     YEAR(date) AS fiscal_year,
# MAGIC     MONTH(date) AS fiscal_month,
# MAGIC     CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
# MAGIC     CASE WHEN DAY(date) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
# MAGIC     CASE WHEN date = LAST_DAY(date) THEN TRUE ELSE FALSE END AS is_month_end
# MAGIC   FROM (
# MAGIC     SELECT sequence(DATE('2020-01-01'), DATE('2030-12-31'), INTERVAL 1 DAY) AS dates
# MAGIC   ) LATERAL VIEW explode(dates) AS date
# MAGIC ) AS s
# MAGIC ON t.date = s.date
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# DBTITLE 1,Auto-Discover Workspaces
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max
from datetime import datetime

# Auto-discover workspaces from billing and audit system tables
# This populates dim_workspace from actual system table data
billing_workspaces = spark.sql("""
    SELECT DISTINCT
        workspace_id,
        account_id,
        cloud,
        NULL AS workspace_name,
        NULL AS workspace_url,
        NULL AS region,
        NULL AS environment,
        NULL AS business_unit,
        NULL AS department,
        NULL AS cost_center,
        NULL AS owner,
        'Active' AS status,
        NULL AS created_date
    FROM system.billing.usage
    WHERE workspace_id IS NOT NULL
""")

audit_workspaces = spark.sql("""
    SELECT DISTINCT
        workspace_id,
        account_id,
        NULL AS cloud,
        NULL AS workspace_name,
        NULL AS workspace_url,
        NULL AS region,
        NULL AS environment,
        NULL AS business_unit,
        NULL AS department,
        NULL AS cost_center,
        NULL AS owner,
        'Active' AS status,
        NULL AS created_date
    FROM system.access.audit
    WHERE workspace_id IS NOT NULL AND workspace_id != '0'
""")

# Union both sources
discovered_workspaces = billing_workspaces.unionByName(audit_workspaces).dropDuplicates(['workspace_id'])

# Add metadata
discovered_workspaces = discovered_workspaces.withColumn('last_updated_timestamp', current_timestamp())

print(f"Discovered {discovered_workspaces.count()} unique workspaces from system tables")

# MERGE into dim_workspace
discovered_workspaces.createOrReplaceTempView('vw_discovered_workspaces')

spark.sql("""
    MERGE INTO finops.dims.dim_workspace AS t
    USING vw_discovered_workspaces AS s
    ON t.workspace_id = s.workspace_id
    WHEN MATCHED AND t.status = 'Active' THEN UPDATE SET
        t.cloud = COALESCE(s.cloud, t.cloud),
        t.account_id = COALESCE(s.account_id, t.account_id),
        t.last_updated_timestamp = current_timestamp()
    WHEN NOT MATCHED THEN INSERT *
""")

print("Workspace master populated. Users can manually update workspace_name, environment, business_unit, etc. via SQL UPDATE statements.")
print("Example: UPDATE finops.dims.dim_workspace SET workspace_name = 'My Workspace', environment = 'PROD' WHERE workspace_id = '123456789'")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Catalog Structure Summary
# MAGIC
# MAGIC | Schema | Tables |
# MAGIC |--------|--------|
# MAGIC | finops.bronze | bronze_billing_usage, bronze_billing_prices, bronze_audit_events, bronze_workspace_master |
# MAGIC | finops.silver | silver_billing_usage, fact_databricks_cost, fact_audit_event, fact_cost_allocation, fact_cost_anomaly |
# MAGIC | finops.gold | gold_workspace_daily_cost, gold_workspace_monthly_cost, gold_sku_cost, gold_user_cost, gold_job_cost, gold_cluster_cost, gold_audit_summary |
# MAGIC | finops.dims | dim_workspace, dim_date, dim_environment, dim_user, dim_sku |
# MAGIC | finops.meta | dq_execution_log, framework_execution_log, pipeline_watermarks |
# MAGIC
# MAGIC **Run this notebook first** before any other notebook in the pipeline. It creates all required tables.
# MAGIC Workspace names and business metadata (environment, cost_center, etc.) should be maintained manually in `finops.dims.dim_workspace` after auto-discovery.

# COMMAND ----------

# DBTITLE 1,Run Full Pipeline (02-12)
import time

notebooks = [
    "02_ingest_billing_usage",
    "03_ingest_billing_prices",
    "04_ingest_audit_logs",
    "05_transform_billing",
    "06_calculate_cost",
    "07_transform_audit",
    "08_cost_allocation",
    "09_data_quality",
    "10_anomaly_detection",
    "11_gold_aggregations",
    "12_dashboard_views",
]

base_path = "/Users/srimantapal205@gmail.com/databricks_finops_platform"

for i, nb in enumerate(notebooks, 1):
    print(f"\n{'='*60}")
    print(f"[{i}/{len(notebooks)}] Running {nb}...")
    print(f"{'='*60}")
    t0 = time.time()
    try:
        result = dbutils.notebook.run(f"{base_path}/{nb}", 900)
        elapsed = time.time() - t0
        print(f"✓ {nb} completed in {elapsed:.1f}s")
    except Exception as e:
        print(f"✗ {nb} FAILED: {str(e)[:500]}")
        raise

print(f"\n{'='*60}")
print("Pipeline complete! All 11 notebooks executed successfully.")
print(f"{'='*60}")