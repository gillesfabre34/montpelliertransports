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
| **08_window_functions** | Cumulative metrics (D2), Lag/Lead (D3) | `cumulative_metrics.py`, `lag_lead.py` | ✅ |
| **09_time_series** | Time bucket, rolling window | `time_bucket.py`, `rolling_window.py` | ✅ |
| **10_mini_pipeline** | Mini pipeline (H): top products, top users, revenue by country | `pipeline.py` | ❌ |
| **11_vehicle_positions_pandas** | Real vehicle positions schema (Bronze mock) with pandas: exploration, D1 dedup, time buckets | `exploration.py`, `dedup_latest_by_entity_trip_pandas.py`, `time_bucket_route_stats_pandas.py` | ❌ |
| **12_spark_bronze_batch** | Spark Bronze (batch) on mock/real Bronze data | `bronze_from_mocks.py` | ❌ |
| **13_spark_silver_dedup_clean** | Spark Silver: D1 dedup + basic quality rules | `silver_dedup_clean.py` | ❌ |
| **14_spark_gold_aggregations** | Spark Gold: hourly and daily route metrics for BI | `gold_route_metrics.py` | ❌ |
| **15_airflow_pipeline_design** | Airflow DAG design (Bronze → Silver → Gold) | `vehicle_positions_dag_design.py` | ❌ |
| **toptal/** | Toptal-style technical interview exercises (Python + data) | Levels 1–5, see below | ✅ |

**Status:** ✅ done (core exercises in the directory implemented) · ❌ to do (at least one exercise not started).

### Toptal-style exercises (`toptal/`)

Short, self-contained exercises for data engineering interviews. Each file has instructions, example I/O, a function signature to implement, a `print` to verify, and corner-case tests. See `toptal/README.md` for the full list.

| Level | Focus |
|-------|--------|
| **1** | Basic Python: lists, dicts, sets, averages, Counter, defaultdict |
| **2** | Data transformation: joins, grouping, sorting |
| **3** | Top-K: heapq, streaming thinking |
| **4** | Data engineering: first non-repeating, sliding window, duplicate events in time window |
| **5** | Interview gaps: `itertools.groupby`, `deque`, generators (`yield`), `bisect`, `dict.get`/`setdefault`, `min`/`max(key=)`, `any`/`all`, `reduce`, `zip`, multi-criteria sort, `next(..., default)`, log parsing |

The core joins (hash join, enrichment) in 02_joins are done; `broadcast_join` is optional (Spark-style optimization).

To run a script from the project root:  
`python training/01_fundamentals/filter_blacklist.py`  
For **06_skew** (imports between files in the same folder), run from the root:  
`python training/06_skew/salt_events.py` or `python training/06_skew/top_k_after_salt.py` — the script’s directory is on the path, so local imports work.  
For **toptal** exercises: `python training/toptal/5_01_groupby_total_per_date_user.py` (or any `N_MM_*.py`); run tests with `pytest training/toptal/5_*.py -v` once implemented.

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
| Cumulative (D2) / Lag-Lead (D3) | partition → order → cumsum, lag, lead | 08_window_functions |
| Time bucket / Rolling window | aggregate by hour/day, sum over last N rows | 09_time_series |

Real-data patterns (vehicle positions, mock Bronze from Azure) reuse the same ideas in 11_vehicle_positions_pandas and extend them to Spark and DAG design in 12–15.

---

## 2️⃣ Introduced (in progress)

**Window functions:** `PARTITION BY`, `ORDER BY`, pattern partition → order → rank/filter (D1), window aggregate (D2), lag/lead (D3). **D1** `dedup_keep_last`, **D2** `cumulative_metrics`, **D3** `lag_lead` done. **09_time_series** (time bucket, rolling window) done. **Next:** `05_top_k/top_k_distributed.py`, `10_mini_pipeline/pipeline.py`, real-data practice in `11_vehicle_positions_pandas/`, and Spark/DAG extensions in 12–15.

---

## 📚 Reference

Modern data pipelines (Spark, Flink, Kafka, data warehouses) rely on a small set of algorithmic patterns. The structure above covers the main ones up to the move to PySpark.

---

## Exercises still to do (in order)

1. `11_vehicle_positions_pandas/` — apply patterns to the real vehicle positions schema (mock Bronze data):
   - `exploration.py` — read Parquet from mock/Azure, basic exploration, stats by route.
   - `dedup_latest_by_entity_trip_pandas.py` — D1: KEEP LAST per (entity_id, trip_id) using pandas.
   - `time_bucket_route_stats_pandas.py` — time buckets + aggregations per (route_id, bucket).
2. `12_spark_bronze_batch/bronze_from_mocks.py` — Spark Bronze (batch) on mock/real Bronze data:
   - create a SparkSession for local batch.
   - implement `read_bronze_batch` (Parquet vs Delta) and `print_bronze_overview`.
3. `13_spark_silver_dedup_clean/silver_dedup_clean.py` — Spark Silver:
   - implement `deduplicate_keep_last_entity_trip_spark` (D1 window).
   - implement `apply_silver_quality_rules` (coordinates, speed, nulls).
   - implement `build_silver_dataframe` (read Bronze → dedup → quality).
4. `14_spark_gold_aggregations/gold_route_metrics.py` — Spark Gold:
   - implement `compute_route_hourly_speed` (route_id, event_hour, metrics).
   - implement `compute_daily_route_activity` (event_date, route_id, metrics).
   - implement `build_gold_dataframes` (Silver → hourly + daily).
5. `15_airflow_pipeline_design/vehicle_positions_dag_design.py` — DAG design:
   - design a dict-based DAG spec for Bronze → Silver → Gold.
   - implement a console-friendly formatter for this spec.
6. `05_top_k/top_k_distributed.py` — F1 distributed top-K (local top-K per partition, then merge).
7. `10_mini_pipeline/pipeline.py` — H mini pipeline (top products, top users, revenue by country, etc.). **Optional**: synthesis exercise; useful to anchor the “one input / multiple outputs pipeline” idea before moving fully to PySpark, but not mandatory if you go directly to Spark.

**Optional** (useful for Spark but not required to complete the path):  
`02_joins/broadcast_join.py` — F3 broadcast join (small table in memory, join with large table).
