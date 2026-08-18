# 🍎 Apple Consumer Behavior Analysis — PySpark ETL Framework

A modular, enterprise-grade Data Engineering project built in **Databricks** utilizing **PySpark**, **Delta Lake**, and **Object-Oriented Programming (OOP)** design patterns. 

This repository implements an end-to-end ETL framework using **Factory Patterns** and **Separation of Concerns** to ingest multi-format datasets (Delta Lake & Parquet), execute complex consumer behavior transformations using Spark SQL & Window Functions, and partition outputs into target data sinks.

---

## 📌 Architecture Overview

The pipeline strictly decouples **Data Ingestion (Extraction)**, **Business Logic (Transformation)**, and **Data Persistence (Loading)** using reusable factory components:

```
┌─────────────────────────┐
│     DATA SOURCES        │
│  - Customer (Delta)     │
│  - Product (Parquet)    │
│  - Transactions (Parq)  │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ apple_reader_factory    │ ◄── Dynamic Reader Factory
│      & Extractor        │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│       Transform         │ ◄── Business Logic & Window Functions
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│     loader_factory      │ ◄── Dynamic Loader Factory (Parquet / Delta)
│       & Loader          │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│     TARGET VOLUMES      │
│  /Volumes/upskill/...   │
└─────────────────────────┘
```

---

## 📂 Repository Structure

```
├── apple_data_analysis.ipynb    # Main pipeline runner orchestrating class calls
├── apple_reader_factory.ipynb   # Factory pattern for dynamic source data readers
├── Extractor.ipynb             # Data extraction functions for CSV, Parquet & Delta
├── transform.ipynb             # Core PySpark transformation logic for analytical cases
├── loader_factory.ipynb        # Factory pattern for configurable data writers & partitioning
└── loader.ipynb                # High-level persistence orchestrator
```

---

## 💡 Analytical Use Cases Implemented

The `transform.ipynb` module handles five specific consumer analytics cases using advanced PySpark windowing, aggregations, and joins:

1. **Sequential Purchase Analysis:** Identifies customers who purchased AirPods *after* purchasing an iPhone using `row_number()` ordering.
2. **Exclusive Category Buyers:** Filters for customers whose purchasing footprint consists *strictly* of iPhones and AirPods.
3. **Post-Initial Purchase History:** Extracts all subsequent products purchased by customers, excluding their initial onboarding transaction.
4. **Time Lag Analysis:** Calculates the exact average lag time (in days) between buying an iPhone and acquiring AirPods using `datediff()` and `pivot()`.
5. **Top Category Revenue Leaders:** Ranks the top 3 selling products per category by total revenue using `DENSE_RANK()` over category partitions.

---

## 🛠️ Tech Stack & Key Concepts

* **Engine:** PySpark / Spark SQL
* **Platform:** Databricks Free Edition & Unity Catalog Volumes
* **Formats:** Delta Lake (`.delta`), Parquet (`.parquet`), CSV
* **Design Patterns:** Factory Pattern, Extractor-Transform-Loader (ETL) Architecture, Object-Oriented Programming (OOP)
* **Optimization:** Partitioning, Windowing (`PARTITION BY`, `ORDER BY`), Aggregations

---

## 🚀 How to Run in Databricks

1. Clone or import this repository into your **Databricks Workspace**.
2. Upload source datasets:
   * `customer` to Delta Table format in Unity Catalog / DBFS.
   * `product` & `transactions` to Parquet files.
3. Execute notebooks in dependency order or run `apple_data_analysis.ipynb` as the master orchestrator.

---

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).
