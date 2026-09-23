# Databricks Multi-Workspace Audit Log, Usage & Billing Monitoring Platform

## Objective

Design and implement a production-grade **Databricks Multi-Workspace Audit, Usage, Cost and Billing Monitoring Platform**.

The solution must provide centralized monitoring across multiple Databricks workspaces using Databricks System Tables and should allow users to analyze:

* Workspace-level cost
* Workspace usage
* DBU consumption
* SKU-level billing
* Compute/resource usage
* User and service-principal activity
* Audit events
* Job activity
* Cluster activity
* SQL Warehouse activity
* Cost trends
* Daily/monthly spending
* Abnormal cost spikes
* Top users
* Top jobs
* Top clusters
* Top SKUs
* Failed/suspicious activities
* Cost allocation using tags
* Workspace comparison

The solution must be scalable enough to support **10s or 100s of Databricks workspaces**.

---

# 1. Workspace Master

Create a centralized workspace master table.

### Table: dim_workspace

| Column                 | Description                    |
| ---------------------- | ------------------------------ |
| workspace_id           | Unique Databricks workspace ID |
| workspace_name         | Workspace name                 |
| workspace_url          | Workspace URL                  |
| account_id             | Databricks account ID          |
| cloud                  | AWS/Azure/GCP                  |
| region                 | Workspace region               |
| environment            | DEV/TEST/UAT/PROD              |
| business_unit          | Business unit                  |
| department             | Department                     |
| cost_center            | Cost center                    |
| owner                  | Workspace owner                |
| status                 | Active/Inactive                |
| created_date           | Workspace creation date        |
| last_updated_timestamp | Last update                    |

Workspace name must be resolved from the available Databricks workspace/account metadata rather than manually duplicating names in every billing record.

---

# 2. Required Databricks Source System Tables

Use Databricks System Tables wherever available.

Primary sources should include:

### Billing

`system.billing.usage`

Capture:

* workspace_id
* usage_start_time
* usage_end_time
* usage_date
* sku
* cloud
* usage_unit
* usage_quantity
* record_type
* custom_tags
* identity_metadata
* usage_metadata
* billing_origin_product
* product_features
* job/cluster/warehouse related metadata where available

### Pricing

`system.billing.list_prices`

Capture pricing information required to calculate estimated cost:

* sku
* price_start_time
* price_end_time
* pricing information
* currency
* effective price

Do not hard-code Databricks pricing.

Pricing must be joined using SKU and the applicable effective pricing period.

### Audit

`system.access.audit`

Capture:

* event_time
* action_name
* user identity
* service principal
* workspace information
* source IP
* user agent
* request parameters
* response/status information
* object/resource information
* audit event details

The implementation must accommodate schema changes in audit event payloads.

---

# 3. Recommended Data Architecture

Implement the following architecture:

Source System Tables
|
v
Bronze Layer
|
v
Silver Layer
|
v
Gold Layer
|
+----------------------+
|                      |
v                      v
Billing/Cost Dashboard     Audit Dashboard
|
v
Workspace Monitoring Dashboard

Use a **Medallion Architecture**.

---

# 4. Bronze Layer

Create append-oriented raw ingestion tables.

### Tables

* bronze_billing_usage
* bronze_billing_prices
* bronze_audit_events
* bronze_workspace_master

Requirements:

* Preserve source records
* Add ingestion_timestamp
* Add ingestion_date
* Add source_system
* Add batch_id
* Maintain source lineage
* Avoid unnecessary transformations
* Support incremental processing

---

# 5. Silver Layer

Create cleansed and standardized tables.

### Table: silver_billing_usage

Required columns:

* usage_date
* usage_start_time
* usage_end_time
* workspace_id
* workspace_name
* environment
* cloud
* region
* sku
* usage_unit
* usage_quantity
* record_type
* billing_origin_product
* job_id
* cluster_id
* warehouse_id
* user_id
* service_principal_id
* custom_tags
* ingestion_timestamp

Handle missing fields gracefully because different Databricks products may provide different metadata.

---

# 6. Billing Calculation

Create a normalized billing fact table.

### Table: fact_databricks_cost

Columns:

* cost_id
* usage_date
* usage_start_time
* usage_end_time
* workspace_id
* workspace_name
* sku
* usage_unit
* usage_quantity
* unit_price
* currency
* estimated_cost
* record_type
* billing_origin_product
* job_id
* cluster_id
* warehouse_id
* user_id
* cost_center
* environment
* business_unit
* ingestion_timestamp

### Cost calculation

Calculate:

`estimated_cost = usage_quantity × applicable_unit_price`

Pricing must be selected based on:

1. SKU
2. Cloud
3. Applicable effective pricing period
4. Usage timestamp

Do not assume one static price for a SKU.

Clearly distinguish:

* DBU usage
* Estimated cost
* Actual invoice/billed amount, if available

Never represent estimated cost as actual invoice cost.

---

# 7. Audit Fact Table

Create:

### fact_audit_event

Columns:

* audit_event_id
* event_time
* event_date
* workspace_id
* workspace_name
* user_name
* user_type
* service_principal
* action_name
* request_id
* source_ip
* user_agent
* object_type
* object_id
* object_name
* response_status
* request_parameters
* audit_details
* ingestion_timestamp

Create normalized dimensions where possible while preserving the original raw audit payload.

---

# 8. Dimensions

Create reusable dimensions.

### dim_workspace

Workspace information.

### dim_user

* user_id
* user_name
* email
* user_type
* department
* business_unit
* status

### dim_sku

* sku
* product
* billing_origin_product
* cloud
* usage_unit
* description

### dim_date

Standard calendar dimension.

Include:

* date
* day
* week
* month
* quarter
* year
* month_name
* fiscal_year
* fiscal_month

### dim_environment

* environment
* description

Examples:

DEV
TEST
UAT
PROD

---

# 9. Cost Allocation

Implement cost allocation using workspace metadata and Databricks custom tags.

Support dimensions such as:

* Workspace
* Environment
* Business Unit
* Department
* Application
* Project
* Cost Center
* Owner
* Job
* Cluster
* SQL Warehouse
* SKU

Create a normalized cost allocation model.

### fact_cost_allocation

Include:

* allocation_date
* workspace_id
* cost_center
* business_unit
* project
* application
* environment
* sku
* usage_quantity
* estimated_cost

---

# 10. Incremental Processing

The framework must be incremental.

Do not reload all historical records every day.

Use:

* ingestion watermark
* event timestamp
* usage date
* MERGE
* Delta Lake
* checkpointing where applicable

Handle:

* late-arriving data
* duplicate records
* updates
* deleted/deactivated workspaces
* schema evolution

Implement idempotent pipelines.

---

# 11. Data Quality Framework

Implement automated DQ checks.

Checks should include:

### Billing

* Missing workspace_id
* Missing SKU
* Negative usage
* Zero usage
* Missing price
* Missing currency
* Duplicate billing records
* Invalid usage dates
* Invalid price-effective dates

### Audit

* Missing event_time
* Missing workspace_id
* Missing action_name
* Duplicate events
* Invalid user information

### Workspace

* Duplicate workspace IDs
* Missing workspace names
* Inactive workspace receiving usage
* Workspace without cost-center mapping

Create:

### dq_execution_log

Columns:

* dq_run_id
* execution_timestamp
* table_name
* check_name
* total_records
* failed_records
* status
* error_message

---

# 12. Cost Anomaly Detection

Implement automated anomaly detection.

Identify:

* Sudden daily cost increase
* Workspace cost spike
* SKU cost spike
* Unusual DBU consumption
* Weekend usage
* After-hours usage
* Unusual user activity
* Unexpected SQL Warehouse cost
* Unexpected job cost
* Unexpected cluster cost

Create:

### fact_cost_anomaly

Columns:

* anomaly_id
* detection_date
* workspace_id
* workspace_name
* anomaly_type
* metric
* baseline_value
* actual_value
* deviation_percentage
* severity
* detected_timestamp
* status

Severity:

* Critical
* High
* Medium
* Low

---

# 13. Gold Aggregation Tables

Create optimized tables for dashboard consumption.

### gold_workspace_daily_cost

* usage_date
* workspace_id
* workspace_name
* environment
* total_usage
* total_cost
* previous_day_cost
* cost_change
* cost_change_percentage

### gold_workspace_monthly_cost

* month
* workspace_id
* workspace_name
* total_usage
* total_cost
* previous_month_cost
* month_over_month_percentage

### gold_sku_cost

* usage_date
* workspace_id
* sku
* usage_quantity
* estimated_cost

### gold_user_cost

* usage_date
* workspace_id
* user_name
* usage_quantity
* estimated_cost

### gold_job_cost

* usage_date
* workspace_id
* job_id
* job_name
* usage_quantity
* estimated_cost

### gold_cluster_cost

* usage_date
* workspace_id
* cluster_id
* cluster_name
* usage_quantity
* estimated_cost

### gold_audit_summary

* event_date
* workspace_id
* action_name
* user_name
* event_count
* failed_event_count

---

# 14. Modern Interactive Monitoring Dashboard

Create a modern enterprise-grade dashboard.

The dashboard should have a clean dark/light theme, KPI cards, interactive charts, drilldowns, filters and cross-filtering.

---

## Dashboard Page 1 — Executive Overview

### KPI Cards

Display:

1. Total Cost — Current Month
2. Previous Month Cost
3. Cost Change %
4. Total DBU Usage
5. Active Workspaces
6. Active Users
7. Total Jobs
8. Total Audit Events
9. Failed Audit Events
10. Cost Anomalies

Each KPI should support:

* Current value
* Previous-period comparison
* Percentage change
* Trend indicator

---

## Cost Trend

Create a line/area chart:

**Daily Databricks Cost**

X-axis:

Date

Y-axis:

Estimated Cost

Allow users to switch between:

* Daily
* Weekly
* Monthly

---

## Workspace Cost Comparison

Create a horizontal bar chart:

Workspace Name vs Cost

Allow sorting:

* Highest cost
* Lowest cost
* Highest growth
* Highest DBU usage

---

## Cost by Environment

Create:

DEV vs TEST vs UAT vs PROD

Use a donut/bar visualization.

---

## Cost by SKU

Show:

SKU → Usage → Cost → Cost %

Provide drilldown from:

SKU → Workspace → Job/Cluster/User

---

# Dashboard Page 2 — Workspace Monitoring

Provide workspace selector.

Filters:

* Workspace
* Environment
* Region
* Cloud
* Business Unit
* Cost Center
* Date Range

Display:

### Workspace KPI

* Monthly Cost
* Daily Cost
* DBU
* Users
* Jobs
* Clusters
* SQL Warehouses
* Audit Events

### Workspace Cost Trend

Daily/monthly cost trend.

### Workspace Cost Breakdown

Break down by:

* SKU
* Product
* Job
* Cluster
* SQL Warehouse
* User
* Tags

### Workspace Ranking Table

Columns:

| Workspace | Environment | Cost | DBU | MoM % | Users | Jobs | Audit Events |
| --------- | ----------- | ---: | --: | ----: | ----: | ---: | -----------: |

Enable conditional formatting.

---

# Dashboard Page 3 — Billing & Cost Analysis

Create detailed billing analysis.

Visuals:

1. Cost by SKU
2. Cost by Workspace
3. Cost by Product
4. Cost by Environment
5. Cost by Business Unit
6. Cost by Cost Center
7. Cost by Application
8. Cost by User
9. Cost by Job
10. Cost by Cluster

Provide drill-through capability.

Example:

Workspace
→ SKU
→ Job
→ Cluster
→ User
→ Daily Usage

---

# Dashboard Page 4 — Audit Monitoring

Create a security/operational audit dashboard.

### KPI Cards

* Total Events
* Unique Users
* Unique Workspaces
* Failed Events
* Admin Events
* Service Principal Events

### Charts

1. Audit events by hour
2. Audit events by day
3. Events by workspace
4. Events by user
5. Events by action
6. Failed events
7. Admin activity
8. Service-principal activity
9. Source IP activity
10. Top API/action operations

### Audit Detail Table

Columns:

| Time | Workspace | User | User Type | Action | Object | IP | Status |
| ---- | --------- | ---- | --------- | ------ | ------ | -- | ------ |

Allow drill-through into complete audit details.

---

# Dashboard Page 5 — User & Activity Monitoring

Show:

* Top users by cost
* Top users by DBU
* Most active users
* Most frequent actions
* Failed actions
* Service principal activity
* User activity by workspace

Table:

| User | Workspace | DBU | Cost | Audit Events | Failed Events | Last Activity |
| ---- | --------- | --: | ---: | -----------: | ------------: | ------------- |

---

# Dashboard Page 6 — Job & Compute Cost

### Job Monitoring

Display:

* Job Name
* Job ID
* Workspace
* Run Count
* DBU
* Estimated Cost
* Average Cost/Run
* Failure Count
* Duration

### Cluster Monitoring

Display:

* Cluster
* Workspace
* Cluster Type
* DBU
* Cost
* Runtime
* Owner
* Environment

### SQL Warehouse Monitoring

Display:

* Warehouse
* Workspace
* Usage
* Cost
* Query count
* Owner

---

# Dashboard Page 7 — Cost Anomaly Monitoring

Display anomaly cards:

* Critical
* High
* Medium
* Low

Create anomaly table:

| Date | Workspace | Metric | Baseline | Actual | Variance % | Severity | Status |
| ---- | --------- | ------ | -------: | -----: | ---------: | -------- | ------ |

Provide drill-through into the underlying billing records.

---

# 15. Interactive Dashboard Filters

Global filters:

* Date Range
* Workspace
* Workspace ID
* Workspace Name
* Environment
* Cloud
* Region
* Business Unit
* Department
* Cost Center
* Application
* Project
* SKU
* User
* Service Principal

Filters must dynamically update all applicable visuals.

---

# 16. Dashboard Interactions

Implement:

* Cross-filtering
* Cross-highlighting
* Drill-down
* Drill-through
* Tooltips
* Date hierarchy
* Workspace hierarchy
* Cost hierarchy
* Dynamic KPI cards
* Top-N selection
* Searchable workspace selector
* Export capability

Example drill path:

**Total Cost**

→ Workspace

→ SKU

→ Job

→ Cluster

→ User

→ Raw Billing Record

---

# 17. Workspace Comparison

Provide a dedicated comparison feature.

Users should be able to select multiple workspaces.

Compare:

* Cost
* DBU
* Cost growth
* Users
* Jobs
* Audit events
* Failed events
* Cost per user
* Cost per job

Example:

| Metric       | Workspace A | Workspace B | Workspace C |
| ------------ | ----------: | ----------: | ----------: |
| Monthly Cost |             |             |             |
| DBU          |             |             |             |
| Users        |             |             |             |
| Jobs         |             |             |             |
| Audit Events |             |             |             |
| Cost/User    |             |             |             |

Do not hard-code workspace names.

---

# 18. Security

Implement role-based access.

Suggested roles:

### Platform Admin

Access to all workspaces and all data.

### Finance

Access to billing and cost information.

### Security/Audit

Access to audit information.

### Workspace Owner

Access only to assigned workspaces.

### Viewer

Read-only dashboard access.

Implement row-level security where supported.

---

# 19. Performance Requirements

Optimize the solution for large datasets.

Use:

* Delta Lake
* Partitioning where justified
* Liquid clustering where appropriate
* Z-Ordering only where applicable
* Data skipping
* Incremental processing
* Gold aggregation tables
* Materialized/optimized views where appropriate
* Query optimization

Do not over-partition small tables.

Dashboard queries should primarily hit Gold/serving tables rather than repeatedly scanning raw audit logs.

---

# 20. Technical Implementation

Use:

* Databricks SQL
* PySpark
* Delta Lake
* Databricks Workflows
* Databricks System Tables
* Unity Catalog
* SQL Warehouses
* Power BI or Databricks SQL dashboards for visualization

Keep the implementation modular.

Recommended notebooks:

01_workspace_master
02_ingest_billing_usage
03_ingest_billing_prices
04_ingest_audit_logs
05_transform_billing
06_calculate_cost
07_transform_audit
08_cost_allocation
09_data_quality
10_anomaly_detection
11_gold_aggregations
12_dashboard_views

---

# 21. Monitoring of the Monitoring Platform

Create a framework monitoring table:

### framework_execution_log

Columns:

* run_id
* pipeline_name
* notebook_name
* start_time
* end_time
* duration_seconds
* records_read
* records_inserted
* records_updated
* records_rejected
* status
* error_message

Dashboard should also monitor the health of the data pipeline itself.

---

# 22. Expected Final Deliverables

Generate:

1. Complete architecture
2. Source-to-target mapping
3. Unity Catalog structure
4. Bronze tables
5. Silver tables
6. Gold tables
7. Dimension tables
8. Fact tables
9. SQL DDL
10. PySpark transformation code
11. Incremental processing logic
12. MERGE logic
13. Data quality framework
14. Cost calculation logic
15. Anomaly detection logic
16. Audit log transformation
17. Dashboard SQL queries
18. Dashboard wireframe
19. KPI definitions
20. Dashboard filters
21. Drill-down/drill-through design
22. Security/RLS design
23. Performance optimization strategy
24. Deployment strategy
25. Testing strategy

---

# 23. Important Design Rules

* Never hard-code workspace names.
* Workspace ID must be the primary workspace reference.
* Workspace name must come from centralized workspace metadata.
* Never hard-code Databricks pricing.
* Use effective pricing periods.
* Clearly distinguish usage, estimated cost and actual billing.
* Preserve raw audit information.
* Do not expose sensitive audit data unnecessarily.
* Make all pipelines idempotent.
* Support schema evolution.
* Handle late-arriving records.
* Support multiple workspaces.
* Support multiple clouds where applicable.
* Design for historical reporting.
* Optimize dashboard queries.
* Use UTC for raw event timestamps and provide local-time reporting where required.
* Include data-quality and pipeline-health monitoring.
* Make the dashboard interactive and production-ready rather than a collection of static charts.

---

# Final User Experience

The final platform should allow an executive or platform administrator to open the dashboard and immediately answer:

1. How much are we spending?
2. Which workspace costs the most?
3. Which workspace's cost is increasing?
4. Which SKU is driving the cost?
5. Which jobs/clusters/warehouses are consuming the most?
6. Which business unit owns the cost?
7. Who are the highest-cost users?
8. What happened in the audit logs?
9. Are there unusual activities?
10. Are there cost anomalies?
11. Which workspaces are healthy?
12. Can I drill from total cost down to the underlying usage record?

The solution as a **production-grade centralized Databricks FinOps + Audit Monitoring Platform**, not merely as a reporting dashboard.
