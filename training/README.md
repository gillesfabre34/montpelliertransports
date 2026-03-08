# 🐍 Python / Data Trainings

This folder is for practising **Python** and **data engineering patterns** (algorithms, aggregations, windows, pipelines), with the goal of mastering PySpark and Kafka → Spark → Delta Lake–style pipelines.

Exercises are grouped by **topic** in numbered subdirectories (in progression order). File and function names are explicit so you can see at a glance what each one covers.

---

## 📂 Structure and progression

| Directory | Topic | Files | Status |
|-----------|--------|--------|--------|
| **01_fundamentals** | Filtering, aggregation (manual groupby), basic structures | `filter_blacklist.py`, `aggregate_by_country.py`, `basics_vehicles.py` | ✅ |
| **02_joins** | Hash join, enrichment; optional: broadcast join (F3) | `enrich_orders_with_users.py`, `join_orders_with_users.py`, `broadcast_join.py` | ✅ |
| **03_deduplication** | Deduplication (first occurrence, then latest by timestamp) | `dedup_first_occurrence.py`, `dedup_keep_last.py` | ✅ |
| **04_partitioning** | Logical partitioning (hash → partition) | `partition_by_key.py` | ✅ |
| **05_top_k** | Top-K global and per partition, heap, distributed (F1) | `top_k_global_heap.py`, `top_k_per_partition_heap.py`, `top_k_users_by_country_heap.py`, `top_k_distributed.py` | ❌ |
| **06_skew** | Skew detection, salting, top-K after salt | `detect_skew.py`, `salt_events.py`, `top_k_after_salt.py` | ✅ |
| **07_pandas** | Same patterns in pandas (merge, groupby, agg, head) | `total_amount_by_country.py`, `top_k_global_pandas.py`, `top_k_products_by_country_pandas.py` | ✅ |
| **08_window_functions** | Cumulative metrics (D2), Lag/Lead (D3) | `cumulative_metrics.py`, `lag_lead.py` | ❌ |
| **09_time_series** | Time bucket, rolling window | `time_bucket.py`, `rolling_window.py` | ❌ |
| **10_mini_pipeline** | Mini pipeline (H): top products, top users, revenue by country | `pipeline.py` | ❌ |

**Status:** ✅ done (core exercises in the directory implemented) · ❌ to do (at least one exercise not started).  
The core joins (hash join, enrichment) in 02_joins are done; `broadcast_join` is optional (Spark-style optimization).

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

**Window functions:** `PARTITION BY`, `ORDER BY`, pattern partition → order → rank/filter (D1) or window aggregate (D2). **D1** `dedup_keep_last`, **D2** `cumulative_metrics` done. **Next:** D3 `08_window_functions/lag_lead.py` (lag/lead: compare with previous/next row).

---

## 📚 Reference

Modern data pipelines (Spark, Flink, Kafka, data warehouses) rely on a small set of algorithmic patterns. The structure above covers the main ones up to the move to PySpark.

---

## Exercises still to do (in order)

1. `08_window_functions/lag_lead.py` — **D3** lag/lead (compare with previous/next row).
2. `09_time_series/time_bucket.py` — time bucketing (aggregate by hour/day).
3. `09_time_series/rolling_window.py` — rolling window (e.g. sum over last N rows).
4. `05_top_k/top_k_distributed.py` — F1 distributed top-K (local top-K per partition, then merge).
5. `10_mini_pipeline/pipeline.py` — H mini pipeline (top products, top users, revenue by country, etc.).

**Optional** (useful for Spark but not required to complete the path):  
`02_joins/broadcast_join.py` — F3 broadcast join (small table in memory, join with large table).
