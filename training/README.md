# 🐍 Python / Data Trainings

This folder is for practising **Python** and **data engineering patterns** (algorithms, aggregations, windows, pipelines), with the goal of mastering PySpark and Kafka → Spark → Delta Lake–style pipelines.

Exercises are grouped by **topic** in numbered subdirectories (in progression order). File and function names are explicit so you can see at a glance what each one covers.

---

## 📂 Structure and progression

| Directory | Topic | Files | Main functions |
|-----------|--------|--------|-----------------|
| **01_fundamentals** | Filtering, aggregation (manual groupby), basic structures | `filter_blacklist.py` | `filter_blacklisted_users` |
| | | `aggregate_by_country.py` | `aggregate_by_country` |
| | | `basics_vehicles.py` | `get_late_vehicles`, `average_speed_by_route`, `vehicles_by_route`, `fastest_vehicle_by_route`, etc. |
| **02_joins** | Hash join, enrichment | `enrich_orders_with_users.py` | `enrich_orders_with_users` |
| | | `join_orders_with_users.py` | `join_orders_with_users` |
| **03_deduplication** | Deduplication (first occurrence, then latest by timestamp) | `dedup_first_occurrence.py` | `deduplicate_first_occurrence` |
| | | `dedup_keep_last.py` | `deduplicate_keep_last` **(to do — D1)** |
| **04_partitioning** | Logical partitioning (hash → partition) | `partition_by_key.py` | `partition_by_key` |
| **05_top_k** | Top-K global and per partition, heap | `top_k_global_heap.py` | `top_k_global_heap` |
| | | `top_k_per_partition_heap.py` | `top_k_products_by_country` |
| | | `top_k_users_by_country_heap.py` | `top_k_users_by_country` |
| **06_skew** | Skew detection, salting, top-K after salt | `detect_skew.py` | `detect_skew` |
| | | `salt_events.py` | `salt_events` |
| | | `top_k_after_salt.py` | `top_k_after_salt` |
| **07_pandas** | Same patterns in pandas (merge, groupby, agg, head) | `total_amount_by_country.py` | `total_amount_by_country` |
| | | `top_k_global_pandas.py` | `top_k_active_users` |
| | | `top_k_products_by_country_pandas.py` | `top_k_products_by_country` |

To run a script from the project root:  
`python training/01_fundamentals/filter_blacklist.py`  
For **06_skew** (imports between files in the same folder), run from the root:  
`python training/06_skew/salt_events.py` or `python training/06_skew/top_k_after_salt.py` — the script’s directory is on the path, so local imports work.

---

## 1️⃣ What is already covered

### Python / data algorithms

**Core patterns:** aggregation (manual groupby), hash join, enrichment, filtering, simple deduplication (first occurrence), top-K (global and per partition), heap, logical partitioning, **data skew (detection + salting)**.

**Complexity:** O(N), O(N log K). **Structures:** `dict`, `set`, `defaultdict`, `heap`.

### Analytical patterns already implemented

| Concept | Implementation | Where |
|--------|-----------------|-------|
| Aggregations | groupby → aggregation | 01_fundamentals, 07_pandas |
| Top-K global | heap or sort → head(K) | 05_top_k, 07_pandas |
| Top-K per partition | partition → heap → head(K) | 05_top_k, 07_pandas |
| Skew: detection + salting | threshold, salting, re-aggregation | 06_skew |

---

## 2️⃣ Introduced (in progress)

**Window functions:** `PARTITION BY`, `ORDER BY`, pattern partition → order → rank → filter. To practise with **D1** (`03_deduplication/dedup_keep_last.py`).

---

## 3️⃣ Still to learn

- **D1** — Deduplication “keep latest by timestamp” (statement and signature in `03_deduplication/dedup_keep_last.py`).
- **D2** — Cumulative metrics. **D3** — Lag / Lead.
- **E** — Time series (time bucket, rolling windows).
- **F** — F1 distributed top-K; F2 skew join in Spark; **F3** broadcast join.
- **G** — Moving to PySpark (translating the patterns).
- **H, I** — Mini pipeline, modern pipeline (optional).

---

## 4️⃣ Short progression (PySpark / Airflow goal)

1. **D1** — Implement `deduplicate_keep_last` (pure Python) in `03_deduplication/dedup_keep_last.py`.
2. **D2** — Cumulative metrics (optional).
3. **Move to PySpark** — Redo D1 (and D2 if needed) in PySpark, then apply to the GTFS-RT stream.
4. **Airflow** — DAG(s) to run the producer and Spark job.
5. The rest (D3, E, F, H, I) as needed.

---

## 5️⃣ Suggested next exercise

**D1 — Deduplication “keep latest by timestamp”**  
File: `training/03_deduplication/dedup_keep_last.py`.  
Input: list of events with `event_id`, `timestamp`.  
Goal: keep **one** record per `event_id`, the one with the **latest timestamp** (bronze → silver / Kafka pattern).

---

## 📚 Reference

Modern data pipelines (Spark, Flink, Kafka, data warehouses) rely on a small set of algorithmic patterns. The structure above covers the main ones up to the move to PySpark.
