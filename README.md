# Databricks Learning Project: Two Simple Spark Declarative Pipelines

This project gives you **two beginner-friendly Lakeflow Spark Declarative Pipelines (SDP)** in Azure Databricks:

1. **Pipeline A (SQL): Batch + data quality + top-users analysis**  
2. **Pipeline B (Python): CDC-enabled Medallion pipeline (Bronze/Silver/Gold) + data quality**

Both are designed for learning and can be built directly in the **Lakeflow Pipelines Editor**.

---

## Repository files you can use directly

- SQL pipeline files: `pipelines/sql/01_sample_users.sql`, `pipelines/sql/02_users_cleaned.sql`, `pipelines/sql/03_users_and_bookings.sql`
- Python CDC medallion pipeline: `pipelines/python/medallion_cdc_pipeline.py`
- Demo data setup script: `data/setup_demo_data.sql`

## Why Declarative Pipelines?

Spark Declarative Pipelines let you define **what** datasets should exist (tables/materialized views and expectations), while Databricks manages execution details (dependencies, orchestration, incremental processing).

### Purpose

- Build reliable ETL/ELT with less orchestration code.
- Add data quality rules close to transformations.
- Visualize lineage in the pipeline graph.
- Move from notebook experimentation to repeatable production pipelines.

---

## Prerequisites

Before building either pipeline:

1. Azure Databricks workspace with **Unity Catalog** enabled.
2. Lakeflow Pipelines Editor enabled for your workspace/user.
3. Permissions:
   - Create/attach compute.
   - `USE CATALOG` + `CREATE SCHEMA` (or `ALL PRIVILEGES`) on target catalog.
4. Access to sample data (`samples.wanderbricks.*`) or ability to create demo source tables.

---

## Data Setup (simple, optional but recommended)

If your workspace already has `samples.wanderbricks.users` and `samples.wanderbricks.bookings`, you can use them directly.

If not, run `data/setup_demo_data.sql`, or create your own learning schema and sample tables:

```sql
CREATE CATALOG IF NOT EXISTS learning;
CREATE SCHEMA IF NOT EXISTS learning.sdp_demo;

CREATE OR REPLACE TABLE learning.sdp_demo.users_src (
  user_id BIGINT,
  name STRING,
  email STRING,
  updated_at TIMESTAMP
);

INSERT INTO learning.sdp_demo.users_src VALUES
(1, 'Alice', 'alice@example.com', current_timestamp()),
(2, 'Bob', NULL, current_timestamp()),
(3, 'Charlie', 'charlie@example.com', current_timestamp()),
(4, 'Dina', 'dina@example.com', current_timestamp());

CREATE OR REPLACE TABLE learning.sdp_demo.bookings_src (
  booking_id BIGINT,
  user_id BIGINT,
  amount DECIMAL(10,2),
  booking_ts TIMESTAMP
);

INSERT INTO learning.sdp_demo.bookings_src VALUES
(1001, 1, 125.50, current_timestamp()),
(1002, 1, 80.00, current_timestamp()),
(1003, 3, 210.00, current_timestamp()),
(1004, 4, 45.00, current_timestamp());
```

> You can switch all references between `learning.sdp_demo.*` and `samples.wanderbricks.*` based on what exists in your environment.

---

## Pipeline A (SQL approach)

### Goal

A simple SQL pipeline that:

- Reads users source data.
- Applies a quality check to drop null emails.
- Joins bookings and returns top users by booking count.

### Steps in Lakeflow Pipelines Editor

1. **Create Pipeline** → choose default catalog/schema.
2. Choose **Start with sample code in SQL**.
3. In `transformations/`, add/replace with below files.
4. Click **Run pipeline**.

### SQL transformation 1: `sample_users.sql`

```sql
CREATE OR REFRESH MATERIALIZED VIEW sample_users AS
SELECT user_id, name, email, updated_at
FROM learning.sdp_demo.users_src;
```

### SQL transformation 2: `users_cleaned.sql` (quality expectation)

```sql
-- Drop records with null email
CREATE OR REFRESH MATERIALIZED VIEW users_cleaned
(
  CONSTRAINT non_null_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT *
FROM sample_users;
```

### SQL transformation 3: `users_and_bookings.sql`

```sql
CREATE OR REFRESH MATERIALIZED VIEW users_and_bookings AS
SELECT
  u.name,
  COUNT(b.booking_id) AS booking_count
FROM users_cleaned u
JOIN learning.sdp_demo.bookings_src b
  ON u.user_id = b.user_id
GROUP BY u.name
ORDER BY booking_count DESC
LIMIT 100;
```

### Expected output datasets

- `sample_users`
- `users_cleaned`
- `users_and_bookings`

---

## Pipeline B (Python approach, CDC + Medallion)

### Goal

A CDC-enabled pipeline in Python using Medallion architecture:

- **Bronze**: ingest change events.
- **Silver**: keep clean/latest user record, apply quality checks.
- **Gold**: aggregate business metric.

### CDC source table (for learning)

Create a tiny CDC feed table:

```sql
CREATE OR REPLACE TABLE learning.sdp_demo.users_cdc_events (
  user_id BIGINT,
  name STRING,
  email STRING,
  op STRING,              -- I/U/D
  event_ts TIMESTAMP
);

INSERT INTO learning.sdp_demo.users_cdc_events VALUES
(1, 'Alice', 'alice@example.com', 'I', timestamp('2026-01-01 00:00:00')),
(2, 'Bob', NULL, 'I', timestamp('2026-01-01 00:01:00')),
(1, 'Alice A', 'alice.a@example.com', 'U', timestamp('2026-01-01 00:02:00')),
(2, 'Bob', NULL, 'D', timestamp('2026-01-01 00:03:00')),
(3, 'Charlie', 'charlie@example.com', 'I', timestamp('2026-01-01 00:04:00'));
```

### Steps in Lakeflow Pipelines Editor

1. **Create a second Pipeline**.
2. Choose **Start with sample code in Python**.
3. Add one Python transformation file (for example `medallion_cdc_pipeline.py`).
4. Paste the code below.
5. Run pipeline and inspect graph.

### Python transformation: `medallion_cdc_pipeline.py`

```python
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

# -------------------------
# BRONZE: raw CDC ingestion
# -------------------------
@dp.table(name="bronze_users_cdc")
def bronze_users_cdc():
    return spark.read.table("learning.sdp_demo.users_cdc_events")


# --------------------------------------------------------
# SILVER: latest active user state + quality expectation
# - keep latest event per user_id by event_ts
# - remove deleted records
# - drop rows with null email
# --------------------------------------------------------
@dp.table(name="silver_users")
@dp.expect_or_drop("email_not_null", "email IS NOT NULL")
def silver_users():
    src = spark.read.table("bronze_users_cdc")

    ranked = (
        src.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("event_ts").desc())
            ),
        )
    )

    return (
        ranked
        .filter((F.col("rn") == 1) & (F.col("op") != F.lit("D")))
        .drop("rn")
    )


# ----------------------------------------------
# GOLD: business-friendly aggregate for reporting
# ----------------------------------------------
@dp.table(name="gold_user_booking_summary")
def gold_user_booking_summary():
    users = spark.read.table("silver_users")
    bookings = spark.read.table("learning.sdp_demo.bookings_src")

    return (
        users.join(bookings, "user_id", "left")
        .groupBy("user_id", "name")
        .agg(F.count("booking_id").alias("booking_count"),
             F.coalesce(F.sum("amount"), F.lit(0)).alias("total_amount"))
        .orderBy(F.col("booking_count").desc())
    )
```

### Expected output datasets

- Bronze: `bronze_users_cdc`
- Silver: `silver_users` (quality enforced)
- Gold: `gold_user_booking_summary`

---

## How to verify each pipeline

After a successful run, use SQL queries in a notebook or SQL Editor:

```sql
SELECT * FROM users_cleaned ORDER BY user_id;
SELECT * FROM users_and_bookings ORDER BY booking_count DESC;

SELECT * FROM bronze_users_cdc ORDER BY event_ts;
SELECT * FROM silver_users ORDER BY user_id;
SELECT * FROM gold_user_booking_summary ORDER BY booking_count DESC;
```

You should see:

- Null-email users removed from cleaned/silver layers.
- CDC updates reflected in latest user state (silver).
- Gold table ready for reporting.

---

## Suggested learning exercises

1. Change expectation behavior from `DROP ROW` to `FAIL UPDATE` and observe pipeline behavior.
2. Add another quality rule (for example valid email regex).
3. Add late-arriving CDC event and validate final silver result.
4. Convert pipeline to Databricks Asset Bundles for source control/CI-CD.

---

## Quick mapping: SQL vs Python approach

- Use **SQL pipeline** when transformation logic is SQL-first and easy for analytics engineers.
- Use **Python pipeline** when you need more complex branching/reuse/functions.
- Both support expectations, dependency graphing, and managed orchestration.

