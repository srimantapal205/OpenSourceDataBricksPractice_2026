# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,12 — Dashboard Views
# MAGIC %md
# MAGIC # 12 — Dashboard Views
# MAGIC
# MAGIC Creates reusable SQL views on top of Gold tables for dashboard consumption: executive KPIs, cost trends, workspace rankings, audit summaries, and anomaly summaries.

# COMMAND ----------

# DBTITLE 1,Create dashboard views
import uuid
from datetime import datetime

batch_id = str(uuid.uuid4())
pipeline = "dashboard_views"
start_ts = datetime.now()

views = [
    ("vw_executive_kpis", """
        SELECT
          (SELECT SUM(estimated_cost) FROM finops.silver.fact_databricks_cost
           WHERE date_format(usage_date,'yyyy-MM') = date_format(current_date,'yyyy-MM')) AS current_month_cost,
          (SELECT SUM(estimated_cost) FROM finops.silver.fact_databricks_cost
           WHERE date_format(usage_date,'yyyy-MM') = date_format(add_months(current_date,-1),'yyyy-MM')) AS previous_month_cost,
          (SELECT SUM(usage_quantity) FROM finops.silver.fact_databricks_cost) AS total_dbu,
          (SELECT COUNT(DISTINCT workspace_id) FROM finops.silver.fact_databricks_cost) AS active_workspaces,
          (SELECT COUNT(DISTINCT user_id) FROM finops.silver.fact_databricks_cost WHERE user_id IS NOT NULL) AS active_users,
          (SELECT COUNT(DISTINCT job_id) FROM finops.silver.fact_databricks_cost WHERE job_id IS NOT NULL) AS total_jobs,
          (SELECT COUNT(*) FROM finops.silver.fact_audit_event) AS total_audit_events,
          (SELECT COUNT(*) FROM finops.silver.fact_audit_event WHERE response_status >= 400) AS failed_audit_events,
          (SELECT COUNT(*) FROM finops.silver.fact_cost_anomaly WHERE status = 'Open') AS open_anomalies
    """),
    ("vw_cost_trend_daily", """
        SELECT usage_date, SUM(total_cost) AS daily_cost, SUM(total_usage) AS daily_dbu
          FROM finops.gold.gold_workspace_daily_cost
         GROUP BY usage_date
         ORDER BY usage_date
    """),
    ("vw_cost_by_environment", """
        SELECT environment, SUM(total_cost) AS total_cost
          FROM finops.gold.gold_workspace_daily_cost
         GROUP BY environment
         ORDER BY total_cost DESC
    """),
    ("vw_cost_by_sku", """
        SELECT sku, SUM(usage_quantity) AS usage_quantity, SUM(estimated_cost) AS estimated_cost,
               ROUND(SUM(estimated_cost) * 100 / SUM(SUM(estimated_cost)) OVER (), 2) AS cost_pct
          FROM finops.gold.gold_sku_cost
         GROUP BY sku
         ORDER BY estimated_cost DESC
    """),
    ("vw_top_users", """
        SELECT user_name, SUM(usage_quantity) AS total_dbu, SUM(estimated_cost) AS total_cost
          FROM finops.gold.gold_user_cost
         GROUP BY user_name
         ORDER BY total_cost DESC
         LIMIT 20
    """),
    ("vw_workspace_ranking", """
        SELECT w.workspace_id, w.workspace_name, w.environment,
               COALESCE(d.total_cost, 0) AS cost,
               COALESCE(d.total_usage, 0) AS dbu,
               COALESCE(d.cost_change_pct, 0) AS mom_pct,
               COALESCE(u.user_count, 0)  AS users,
               COALESCE(j.job_count, 0)   AS jobs,
               COALESCE(a.audit_events, 0) AS audit_events
          FROM finops.dims.dim_workspace w
          LEFT JOIN (SELECT workspace_id, SUM(total_cost) AS total_cost, SUM(total_usage) AS total_usage,
                            MAX(cost_change_percentage) AS cost_change_pct
                       FROM finops.gold.gold_workspace_daily_cost GROUP BY workspace_id) d
            ON w.workspace_id = d.workspace_id
          LEFT JOIN (SELECT workspace_id, COUNT(DISTINCT user_name) AS user_count
                       FROM finops.gold.gold_user_cost GROUP BY workspace_id) u
            ON w.workspace_id = u.workspace_id
          LEFT JOIN (SELECT workspace_id, COUNT(DISTINCT job_id) AS job_count
                       FROM finops.gold.gold_job_cost GROUP BY workspace_id) j
            ON w.workspace_id = j.workspace_id
          LEFT JOIN (SELECT workspace_id, SUM(event_count) AS audit_events
                       FROM finops.gold.gold_audit_summary GROUP BY workspace_id) a
            ON w.workspace_id = a.workspace_id
         ORDER BY cost DESC
    """),
    ("vw_audit_by_action", """
        SELECT action_name, COUNT(*) AS event_count,
               COUNT(CASE WHEN response_status >= 400 THEN 1 END) AS failed_events
          FROM finops.silver.fact_audit_event
         GROUP BY action_name
         ORDER BY event_count DESC
    """),
    ("vw_anomaly_summary", """
        SELECT severity, COUNT(*) AS anomaly_count,
               SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS open_count
          FROM finops.silver.fact_cost_anomaly
         GROUP BY severity
         ORDER BY CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END
    """),
]

for view_name, view_sql in views:
    spark.sql(f"CREATE OR REPLACE VIEW finops.gold.{view_name} AS {view_sql}")
    print(f"  Created {view_name}")

end_ts = datetime.now()
dur = (end_ts - start_ts).total_seconds()
spark.sql(f"""
    INSERT INTO finops.meta.framework_execution_log
    VALUES ('{batch_id}', '{pipeline}', '12_dashboard_views',
            TIMESTAMP('{start_ts}'), TIMESTAMP('{end_ts}'), {dur},
            0, 0, 0, 0, 'SUCCESS', NULL)
""")
print(f"Done. {len(views)} views created. Duration={dur:.1f}s")