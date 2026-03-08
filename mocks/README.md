# Mock data (from Azure)

This folder can hold **Parquet files** downloaded from Azure (Bronze layer output from the Spark job). They use the same schema as the data written by `consumer/spark_stream.py` after reading from Kafka and partitioning.

**Preferred option:** instead of copying Parquet files here, you can point the mock source to an **Azure partition path** (see below). That way you use real data already in the cloud without running Kafka or downloading files.

---

## Purpose

- **Work without Kafka**: run PySpark (batch, Silver/Gold, aggregations) without starting Kafka or the producer.
- **Real data**: same format and schema as production; good for development and debugging.

---

## Mock source: Azure path (recommended) or local folder

You can use mock data in two ways:

1. **Azure partition path** (recommended)  
   Set `MOCK_DATA_PATH` to a path in Azure Blob Storage that contains Bronze Parquet data, e.g.:
   - Whole Bronze folder:  
     `wasbs://<container>@<storage_account>.blob.core.windows.net/<bronze_folder>/`
   - One partition:  
     `wasbs://<container>@<storage_account>.blob.core.windows.net/<bronze_folder>/year=2025/month=3/day=8/`

   Same credentials as production (e.g. `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_ACCESS_KEY`). No Kafka needed; you read existing data in the cloud.

2. **Local folder**  
   Set `MOCK_DATA_PATH` to a local path (e.g. `mocks/` or `./mocks/`) where you have put Parquet files downloaded from Azure. Useful when offline or when you want a fixed snapshot.

When `MOCK_DATA_PATH` is set, the consumer runs in **batch mode**: it reads Parquet from that path (Azure or local) instead of streaming from Kafka, then applies the same downstream logic (e.g. write to Delta or run analytics).

---

## Schema

- **Format**: Parquet (Snappy).
- **Columns** (Bronze): `entity_id`, `trip_id`, `route_id`, `latitude`, `longitude`, `bearing`, `speed`, `event_timestamp`, `year`, `month`, `day`.

---

## How to use the mock

### 1. Environment variables (consumer)

Set:

- `MOCK_DATA_PATH=<path>`
  - **Azure**: e.g. `wasbs://mycontainer@mystorage.blob.core.windows.net/bronze/` or `.../bronze/year=2025/month=3/day=8/`
  - **Local**: e.g. `mocks/` or `./mocks/` (relative to the working directory when you run the job)

Optional:

- `MOCK_DATA_FORMAT=parquet` (default) or `delta`  
  Use `delta` when the path is a Delta table (e.g. your Bronze table in Azure). Use `parquet` for a folder of Parquet files (e.g. local `mocks/` or a single partition path).

When `MOCK_DATA_PATH` is set, the Spark consumer runs in batch: it reads from that path instead of Kafka, then prints row count and a sample. No Kafka required. For Azure paths, ensure `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_ACCESS_KEY` are set so Spark can access the storage.

### 2. Ad‑hoc batch PySpark

From a script or notebook (run from project root):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mock").getOrCreate()

# Local
df = spark.read.parquet("mocks/")

# Or Azure (with Azure config already set on Spark)
# df = spark.read.parquet("wasbs://container@account.blob.core.windows.net/bronze/year=2025/month=3/day=8/")

df.show(5)
```

Then chain Silver (dedup, cleaning), Gold (aggregations), analytics, etc.

### 3. File stream (streaming without Kafka)

To keep the streaming API (readStream → writeStream) without Kafka, you can use a folder as a stream source. With a static folder, the stream processes the files once; useful to test writeStream logic (e.g. local Delta).

---

## Adding more mock data

- **Azure**: no copy needed; point `MOCK_DATA_PATH` at the desired container/folder/partition.
- **Local**: add more Parquet files (same schema) under `mocks/`. `spark.read.parquet("mocks/")` will read all `*.parquet` in that directory.
