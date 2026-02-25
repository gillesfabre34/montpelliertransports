## 🚀 Real-Time Public Transport Data Platform (Montpellier)

An end-to-end real-time data platform for Montpellier public transport data, from GTFS-RT ingestion to analytics-ready datasets for BI dashboards.

---

## 🎯 Objective

**Build a real-time data platform that:**

- **Ingests** public transport GTFS-RT data continuously  
- **Streams** events through **Kafka** (Kraft mode) 
- **Processes** them using **PySpark Structured Streaming**  
- **Stores** them in **PostgreSQL (Azure)**  
- **Models** analytics-ready datasets using **dbt**  
- **Orchestrates** pipelines with **Airflow**  
- **Exposes** a clean analytics layer for BI tools  

---

## 🌍 Data Source

**Real-time GTFS-RT feeds from the Montpellier public transport system.**

- **Includes:**
  - Vehicle positions  
  - Trip updates  
  - Service alerts  

- **Format:**
  - Protobuf (GTFS Realtime standard)  
  - Frequently updated (near real-time)  

---

## 🏗️ High-Level Architecture

```text
GTFS-RT API (Montpellier)
        ↓
   Python Producer
        ↓
       Kafka
        ↓
PySpark Structured Streaming
        ↓
 PostgreSQL (Azure)
        ↓
        dbt
        ↓
   BI / Dashboard
```

**Orchestration**: Apache Airflow

---

## 🔧 Tech Stack

### ☁️ Cloud

- Microsoft Azure  
  - Azure Database for PostgreSQL – Flexible Server (free tier for 12 months)

### 🗄 Database

- PostgreSQL (local for development, Azure for cloud deployment)

### 📡 Streaming Layer

- Apache Kafka (Docker or Azure VM)

**Example topics:**

- `vehicle_positions_raw`  
- `vehicle_positions_clean`  
- `trip_updates`  
- `alerts`  

### 🔥 Processing

- Apache Spark  
- PySpark Structured Streaming  

### 🐍 Programming Language

- Python (core language across the platform)

### 📊 Analytics Modeling

- dbt (running on PostgreSQL)

### 🛫 Orchestration

- Apache Airflow (Dockerized)

---

## 🐍 Python Usage Across the Platform

### 1️⃣ Ingestion Layer (Python Producer)

Python service that:

- Polls the GTFS-RT API  
- Parses protobuf messages  
- Converts them to structured JSON  
- Publishes events to Kafka  

**Libraries:**

- `requests`  
- `gtfs-realtime-bindings`  
- `confluent-kafka`  
- `pandas` (for normalization / validation if needed)  

---

### 2️⃣ Streaming Processing (PySpark)

Spark job written in Python that:

- Reads from Kafka topics  
- Parses structured JSON schemas  
- Handles late events  
- Applies window aggregations  
- Performs data cleaning and normalization  
- Writes structured results to PostgreSQL  

**Key concepts:**

- Structured Streaming  
- Event-time processing  
- Watermarking  
- Window functions  
- Aggregations  

---

### 3️⃣ Pandas Usage

Pandas is used for:

- Initial data exploration  
- Transformation prototyping before Spark implementation  
- Unit testing transformations  
- Feature engineering  
- Optional ML experiments  

---

### 4️⃣ Airflow (Orchestration)

Airflow DAGs written in Python handle:

- Ingestion scheduling  
- Spark job execution  
- dbt runs  
- Data quality checks  
- Alerting  

Airflow provides Python-based orchestration for the entire platform.

---

## 🧱 Data Architecture (Medallion Pattern)

### Bronze Layer

- Raw ingested data from Kafka  
- Minimal transformation  
- Append-only  
- Full history preserved  

### Silver Layer

- Cleaned and normalized data:  
  - Deduplication  
  - Timestamp standardization  
  - Schema validation  
  - Geospatial normalization  

### Gold Layer

- Business-ready tables, such as:  
  - Average delay per line  
  - Station congestion metrics  
  - Traffic activity by time window  
  - Peak usage detection  
  - Anomaly detection  
  - Optional forecasting features  

---

## 📊 Example Analytics Use Cases

- Most congested stations  
- Average delay per route  
- Hourly traffic activity  
- Peak usage detection  
- Real-time anomaly detection  
- Transport load evolution over time  
- Optional: prediction of station saturation  

---

## 📁 Suggested Repository Structure

```text
project/
│
├── producer/
│   ├── gtfs_producer.py
│   └── config.py
│
├── spark/
│   ├── stream_processor.py
│   └── schemas.py
│
├── airflow/
│   └── dags/
│       ├── ingestion_dag.py
│       ├── processing_dag.py
│       └── dbt_dag.py
│
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🚀 Project Phases

### Phase 1 – Local Setup

- Kafka (Docker)  
- Spark (Docker)  
- Airflow (Docker)  
- Local PostgreSQL  
- Working Python producer  

### Phase 2 – Streaming Pipeline

- Reliable Spark streaming job  
- Schema management  
- Error handling  
- Writing to PostgreSQL  

### Phase 3 – Cloud Deployment

- PostgreSQL on Azure  
- Secure connections  
- Deployment strategy  

### Phase 4 – Analytics Layer

- dbt models (Bronze → Silver → Gold)  
- Tests  
- Documentation  
- Data quality checks  

### Phase 5 – Advanced

- Great Expectations  
- CI/CD  
- Monitoring  
- ML forecasting  
- Infrastructure automation (Terraform optional)  

