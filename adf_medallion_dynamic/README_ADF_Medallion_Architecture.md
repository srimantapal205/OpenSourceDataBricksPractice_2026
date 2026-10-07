# ADF → Bronze → Silver → Gold: Dynamic Multi-Table Medallion Architecture

## Problem Statement

You are ingesting data from on-premises sources (SQL Server, Oracle, etc.) using **Azure Data Factory (ADF)**. The target is a Databricks Lakehouse with **Bronze / Silver / Gold** layers.

### Constraints

1. **No `raw` or `stg` ADLS container** — There is no separate landing or staging storage container. You cannot stage files in a traditional `raw/` or `stg/` ADLS folder hierarchy.
2. **Must use a specific Unity Catalog and schema** — All Bronze tables must land in a designated UC catalog and schema (e.g., `adf_catalog.bronze`).
3. **Multiple source tables** — N source tables must be ingested dynamically without hardcoding each table.

---

## Solution Overview

### Key Idea: Metadata-Driven Pipeline + UC Volumes as Landing Zone

Since there is no raw/stg ADLS container, we replace it with **Unity Catalog (UC) Volumes** as the landing zone. ADF writes files (Parquet/CSV) directly to a UC Volume. A Databricks notebook then reads a **control/metadata table**, loops over every registered source table, and dynamically creates Bronze → Silver → Gold Delta tables in the correct UC catalog/schema.

```
  ┌──────────┐        ┌─────────────────┐       ┌──────────────────────────────────────┐
  │  On-Prem │        │  ADF Copy Act   │       │  Unity Catalog                        │
  │  Sources │───────▶│  (Parquet sink) │──────▶│                                       │
  │ (N tbls) │        │  → UC Volume    │       │  ┌─────────┐  ┌─────────┐  ┌────────┐│
  └──────────┘        └─────────────────┘       │  │ BRONZE  │→│ SILVER  │→│ GOLD   ││
                                                │  │(Delta)  │  │(Delta)  │  │(Delta) ││
  ┌──────────────────────────────────────┐      │  └─────────┘  └─────────┘  └────────┘│
  │  Control / Metadata Table            │      │  catalog.bronze  catalog.silver  cat.gold│
  │  (maps source → bronze → silver →   │─────▶│                                       │
  │   gold table names + transforms)     │      └──────────────────────────────────────┘
  └──────────────────────────────────────┘
```

### Why UC Volumes Replace the Raw Container

| Traditional (with raw/stg container)         | This Solution (no raw/stg container)                |
|---------------------------------------------|-----------------------------------------------------|
| ADF writes to `abfss://raw@container.dfs...`| ADF writes to `/Volumes/adf_catalog/landing/...`     |
| Bronze = `COPY INTO` from raw container     | Bronze = Auto Loader / `COPY INTO` from UC Volume   |
| Requires ADLS container provisioning        | UC Volume is created within Databricks — no infra   |
| Separate storage accounts for each env      | UC Volume is governed by Unity Catalog               |

---

## Architecture Layers

### Bronze Layer (`adf_catalog.bronze`)

- **Purpose:** Raw ingest — 1:1 copy of source data, no transformations.
- **How:** Auto Loader or `COPY INTO` reads Parquet/CSV files from the UC Volume landing zone.
- **Schema:** Matches source; adds `_ingested_at` and `_source_file` metadata columns.
- **Dynamic:** The notebook reads the control table, loops over each entry, and creates/appends to the Bronze Delta table.

### Silver Layer (`adf_catalog.silver`)

- **Purpose:** Cleansed + conformed — deduplication, type casting, null handling, standardization.
- **How:** `MERGE INTO` from Bronze using the primary key defined in the control table.
- **Dynamic:** Each table's transformation rules (e.g., which columns to trim, which to cast) are stored as SQL fragments in the control table and executed dynamically.

### Gold Layer (`adf_catalog.gold`)

- **Purpose:** Business-ready aggregates and curated tables.
- **How:** SQL transformations (aggregations, joins, window functions) from Silver.
- **Dynamic:** The aggregation SQL is stored in the control table and executed per-table.

---

## The Control / Metadata Table

This is the heart of the **dynamic** approach. Instead of hardcoding each table, we store everything in a control table:

| Column                  | Description                                              |
|-------------------------|----------------------------------------------------------|
| `source_table_name`    | Name of the source table (e.g., `customers`)             |
| `bronze_table_name`    | Full Bronze table name (e.g., `adf_catalog.bronze.customers`) |
| `silver_table_name`    | Full Silver table name                                    |
| `gold_table_name`      | Full Gold table name (NULL = no gold table needed)       |
| `primary_keys`         | Comma-separated PK columns for MERGE (e.g., `customer_id`) |
| `volume_subpath`       | Subfolder in the UC Volume where ADF lands files        |
| `silver_transform_sql` | SQL fragment for Silver cleansing (SELECT with casts)   |
| `gold_transform_sql`    | SQL fragment for Gold aggregation                        |
| `is_active`            | Boolean — whether this table is included in the pipeline  |

---

## How to Run

1. Execute the notebook **`ADFDynamicMedallionPipeline`** in this folder.
2. The notebook will:
   - Create the UC catalog and schemas (bronze, silver, gold) and a landing UC Volume.
   - Create the control/metadata table with 3 sample source tables.
   - Simulate ADF landing by writing Parquet files into the UC Volume.
   - Dynamically build Bronze → Silver → Gold tables.
3. Review the validation output (row counts, sample data).

---

## ADF Configuration Notes (for production)

In production, ADF's Copy Activity is configured as follows:

- **Source:** On-prem SQL Server / Oracle / etc. (via Self-Hosted Integration Runtime)
- **Sink:** Azure Data Lake Storage Gen2 / Databricks Unity Catalog Volume
  - **File format:** Parquet (recommended) or CSV
  - **Sink path:** `/Volumes/adf_catalog/landing/<table_name>/` (one subfolder per source table)
  - **Copy behavior:** Preserve hierarchy / append
- **Pipeline:** A ForEach loop iterates over a list of table names (from a lookup activity that queries the control table or a JSON config).

### ADF → Databricks Handoff

```
ADF Pipeline:
  1. Lookup Activity → get table list (from control table or JSON config)
  2. ForEach Activity → for each table:
     a. Copy Activity → Source: On-prem SQL → Sink: UC Volume (Parquet)
  3. Web Activity / Databricks Notebook Activity → trigger the Databricks notebook
     that builds Bronze → Silver → Gold
```

---

## Key Benefits

- **No ADLS container needed** — UC Volumes serve as the landing zone.
- **Fully dynamic** — Add a new table by inserting one row into the control table; no code changes.
- **Governed** — All tables and volumes are managed by Unity Catalog.
- **Metadata-driven** — Transformations are SQL fragments in the control table, not hardcoded.
