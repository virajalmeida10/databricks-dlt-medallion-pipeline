# 🚀 Databricks DLT Sales Analytics Pipeline

A data engineering pipeline built with **Databricks Delta Live Tables (DLT)** following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## 📌 What This Does

Ingests raw sales data from two regional sources (East + West), cleans and enriches it, and produces a final business analytics table showing **total sales by region and product category**.

---

## 🏗️ Architecture

| Layer | Tables | Purpose |
|-------|--------|---------|
| Bronze | customers_stg, products_stg, sales_stg | Raw ingestion + data quality checks |
| Silver | customers_enr, products_enr, sales_enr | Transformations + CDC upserts (SCD Type 1) |
| Gold | dim_customers, dim_products, fact_sales, business_sales | Star schema + SCD Type 2 + final aggregation |

---

## 🛠️ Tech Stack

- Databricks Delta Live Tables (DLT)
- PySpark
- Delta Lake
- Python 3.x
- SQL (source table setup)

---

## 📂 Project Structure
```
DLT_Root/
├── transformations/
│   ├── bronze/
│   │   ├── ingestion_customers.py
│   │   ├── ingestion_products.py
│   │   └── ingestion_sales.py
│   ├── silver/
│   │   ├── transform_customers.py
│   │   ├── transform_products.py
│   │   └── transform_sales.py
│   └── gold/
│       ├── dim_customers.py
│       ├── dim_products.py
│       ├── fact_sales.py
│       └── business_sales.py
├── utilities/
│   └── utils.py
└── explorations/
    └── sample_exploration.py
```

---

## ⚙️ Setup

1. Run `SQL_DDL.txt` in a Databricks SQL warehouse to create source tables
2. Create a DLT pipeline pointing to the `transformations/` folder
3. Run the pipeline — initial load first, then incremental

---

## 🔑 Key Concepts

- **append_flow** — merges East + West sales into one Bronze table
- **DLT Expectations** — drops bad records at Bronze before they reach Silver
- **create_auto_cdc_flow** — handles insert/update/delete automatically
- **SCD Type 1** — overwrites on change (Silver tables + fact_sales)
- **SCD Type 2** — preserves history on change (dim_customers, dim_products)
- **Star Schema** — fact + dimension tables joined in Gold for analytics

---

## 👤 Author
**Viraj Almeida** — Data Engineer
