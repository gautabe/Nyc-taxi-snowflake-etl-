# 🚕 NYC Taxi ELT Pipeline — Snowflake Medallion Architecture

> End-to-end ELT pipeline built on Snowflake, ingesting 11M+ NYC taxi records through a Bronze → Silver → Gold Medallion architecture with automated CDC orchestration.

---

## 📐 Architecture

![Architecture](docs/architecture.png)

| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| 🥉 Bronze | `RAW_TAXI_RIDES` | 11,282,403 | Raw data + ingestion metadata |
| 🥈 Silver | `CLEAN_TAXI_RIDES` | 11,282,403 | Cleaned, typed, enriched |
| 🥇 Gold | `DAILY_TRIP_SUMMARY` | 31 | Trips per day |
| 🥇 Gold | `HOURLY_PATTERN` | 28 | Trips by day × part of day |
| 🥇 Gold | `WEEKEND_VS_WEEKDAY` | 2 | Weekend vs weekday comparison |

---

## 🛠️ Tech Stack

- **Cloud Data Warehouse** — Snowflake (trial account)
- **Data Source** — CARTO Academy Dataset via Snowflake Marketplace
- **Orchestration** — Snowflake Streams & Tasks (native CDC)
- **Modeling** — Medallion Architecture (Bronze / Silver / Gold)
- **Access Control** — Snowflake RBAC (custom roles)
- **Performance** — Clustering Keys on Silver layer

---

## 📁 Project Structure

```
nyc-taxi-snowflake-elt/
│
├── README.md
├── docs/
│   └── architecture.png
└── sql/
    ├── step_01_setup.sql           # Warehouse, database, schemas, RBAC
    ├── step_02_ingestion_bronze.sql # Source exploration + Bronze ingestion
    ├── step_03_silver.sql          # Data quality audit + Silver transformation
    ├── step_04_gold.sql            # Gold aggregations + business insights
    ├── step_05_tasks.sql           # Streams (CDC) + automated Tasks
    └── step_06_performance.sql     # Clustering keys + query optimization
```

---

## 🚀 Getting Started

### Prerequisites
- A Snowflake account (free trial works)
- Access to **CARTO Academy — Data for tutorials** on Snowflake Marketplace (free)

### Setup

**1. Clone the repo**
```bash
git clone https://github.com/YOUR_USERNAME/nyc-taxi-snowflake-elt.git
```

**2. Run scripts in order** inside a Snowflake worksheet:
```
step_01_setup.sql           → Creates warehouse, database, schemas, roles
step_02_ingestion_bronze.sql → Ingests 11M rows into Bronze layer
step_03_silver.sql          → Cleans and enriches data into Silver layer
step_04_gold.sql            → Builds 3 Gold aggregation tables
step_05_tasks.sql           → Sets up CDC Stream + automated Tasks
step_06_performance.sql     → Applies clustering keys for optimization
```

> ⚠️ In `step_01_setup.sql`, replace `GAUTIER` with your own Snowflake username.

---

## 🔑 Key Concepts Demonstrated

### Medallion Architecture
Three-layer data organization ensuring clean separation of concerns:
- **Bronze** — raw data preserved as-is, with metadata columns (`_ingested_at`, `_source`)
- **Silver** — standardized, typed, enriched data ready for analytics
- **Gold** — pre-aggregated business metrics optimized for consumption

### RBAC (Role-Based Access Control)
Two custom roles reflecting real-world access patterns:
- `ELT_ENGINEER` — full pipeline access (read/write all layers)
- `ELT_ANALYST` — read-only access on Gold layer only

### CDC with Streams & Tasks
Automated pipeline using Snowflake-native features:
- `STREAM_BRONZE_TAXI` — detects new inserts in Bronze (append-only)
- `TASK_BRONZE_TO_SILVER` — runs every 60 minutes, transforms new records
- `TASK_SILVER_TO_GOLD` — triggered after Silver task, refreshes Gold tables

### Clustering Keys
Applied on `CLEAN_TAXI_RIDES` to optimize date-range queries:
```sql
ALTER TABLE NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
    CLUSTER BY (trip_date, day_of_week);
```

---

## 📊 Key Business Insights

From the Gold layer analysis of December 2015 NYC taxi data:

| Finding | Value |
|---------|-------|
| Total trips | 11,282,403 |
| Busiest day of week | Thursday (1.85M trips) |
| Quietest day | Monday (1.37M trips) |
| Highest avg trips/day | Saturday (390K — holiday season effect) |
| Date range | 2015-12-01 → 2015-12-31 |

---

## 📚 What I Learned

- Designing a production-grade Medallion architecture on Snowflake
- Implementing CDC pipelines with Snowflake Streams (append-only mode)
- Chaining Tasks within the same schema to build dependency graphs
- Applying clustering keys and understanding micro-partition optimization
- Enforcing RBAC with principle of least privilege
- Data quality auditing before transformation (null checks, distribution analysis)

---

## 👤 Author

**Seraphin Nomo** — Junior Data Engineer  
Built as a portfolio project to demonstrate Snowflake and ELT engineering skills.

---

## 📄 License

MIT License — feel free to fork and adapt for your own portfolio.
