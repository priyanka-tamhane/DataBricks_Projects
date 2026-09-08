# 🛒 Olist E-Commerce End-to-End Medallion Data Pipeline

An end-to-end Data Engineering platform built on **Databricks** and **Delta Lake** using the **Medallion Architecture (Bronze → Silver → Gold)**. The pipeline ingests, cleans, enriches, and aggregates 9 relational e-commerce datasets (~100k Brazilian orders) and orchestrates execution via a multi-task **Databricks Workflows DAG**.

---
**Databricks Workflows DAG Sequence:**
`01_bronze_ingestion` ➔ `02_silver_transformations` ➔ `03_gold_analytics`

---

## 🛠️ Tech Stack & Key Technologies

* **Compute Engine:** Apache Spark / PySpark
* **Storage Format:** Delta Lake (`format("delta")`)
* **Orchestration:** Databricks Workflows (Jobs)
* **Query Language:** PySpark DataFrame API & ANSI Spark SQL
* **Platform:** Databricks Community Edition

---

## 📂 Project Structure

```text
├── notebooks/
│   ├── 01_bronze_ingestion.py      # Raw CSV ingestion into Bronze Delta tables
│   ├── 02_silver_transformations.py # Data cleansing, casting, joins & category translations
│   └── 03_gold_analytics.sql       # Business aggregations, window functions (LAG), and KPIs
├── data/                            # Raw dataset documentation (Olist E-Commerce)
└── README.md                        # Project documentation


🗄️ Pipeline Architecture Deep Dive
🥉 1. Bronze Layer (Raw Data Ingestion)
Ingests 9 CSV files (orders, order_items, order_payments, customers, products, category_translation, etc.) using explicit StructType schemas to prevent data drift and schema mismatch errors.

Preserves raw source attributes without destructive modifications.

Appends an _ingested_at operational metadata timestamp (current_timestamp()) to every record for lineage and auditability.

🥈 2. Silver Layer (Cleansing & Enrichment)
Schema Enforcement & Casting: Converts raw date strings into standard TimestampType and numeric metrics into DoubleType/IntegerType.

Category Translation & Null Strategy: Joins products with product_category_name_translation using a LEFT JOIN. Missing categories or missing translations are safely handled using coalesce()/fillna() with 'Unknown'.

Text Formatting: Uses regexp_replace() and initcap() to convert snake_case category strings (e.g., cama_mesa_banho) into clean title-case display names (Bed Table Bath).

Data Quality Filters: Filters out invalid records (e.g., non-positive prices or orphan records missing primary keys).

🥇 3. Gold Layer (Business Intelligence & Analytics)
Monthly Revenue & MoM Growth: Uses Spark SQL window functions (LAG() OVER (...)) to calculate monthly total revenue and percentage growth month-over-month.

Geographic Order Distribution: Aggregates order volume, unique customer counts, and average order value (AOV) across Brazilian states.

Top Product Categories: Aggregates total items sold, total revenue, and average price per item across translated product categories for delivered orders.

⚙️ Orchestration & Automated Workflows
The pipeline is orchestrated using Databricks Workflows:

01_bronze_ingestion: Runs as the initial task to load fresh raw files.

02_silver_transformations: Depends on 01_bronze_ingestion; executes data cleaning upon successful Bronze completion.

03_gold_analytics: Depends on 02_silver_transformations; updates business analytical tables once Silver tables are refreshed.

🚀 How to Run in Databricks
Upload Datasets: Place the Olist CSV datasets into your Databricks Volume or DBFS path (/Volumes/upskill/pyspark_learning/datasets/...).

Import Notebooks: Import the three scripts (01_bronze_ingestion, 02_silver_transformations, 03_gold_analytics) into your workspace.

Create Job:

Go to Jobs & Pipelines in Databricks.

Click Create Job and add 3 notebook tasks with explicit linear dependencies (01 → 02 → 03).

Trigger Run: Click Run Now to execute the pipeline end-to-end.
