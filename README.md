## 🚀 Real-Time Public Transport Data Platform (Montpellier)

An end-to-end real-time data platform for Montpellier public transport data, from GTFS-RT ingestion to analytics-ready datasets for BI dashboards.

---

## 🎯 Objective

**Build a real-time data platform that:**

- **Ingests** public transport GTFS-RT data continuously  
- **Streams** events through **Kafka** (Kraft mode) 
- **Processes** them using **PySpark Structured Streaming**  
- **Stores** and models analytics-ready datasets on **Databricks** (Delta Lake)  
- **Uses** **pandas** for exploration, prototyping and local transformations  
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
     Databricks
  (Delta Lake / Spark)
        ↓
   BI / Dashboard
```

**Orchestration**: Apache Airflow

---

## 📍 Où on en est

**État actuel du projet :**

- ✅ **Kafka** fonctionne : le producer Python envoie bien les événements GTFS-RT sur les topics.
- ✅ **Spark** reçoit les événements : le flux est consommé par PySpark Structured Streaming depuis Kafka.

**À venir :** persistance vers Databricks (Delta Lake), modélisation Bronze/Silver/Gold, orchestration Airflow, couche analytics.

---

## 🔧 Tech Stack

### ☁️ Cloud

- Microsoft Azure  
  - **Databricks** (Azure Databricks) – lakehouse, Spark et Delta Lake

### 🗄 Stockage & traitement données

- **Databricks** (Delta Lake pour le stockage, Spark/PySpark pour le traitement et la modélisation).  
  _Contrairement à PostgreSQL (base SQL), Databricks est une plateforme lakehouse (Spark, Delta Lake) dédiée à l’analytique et au big data._

### 📡 Streaming Layer

- Apache Kafka (Docker or Azure VM)

**Example topics:**

- `vehicle_positions_raw`  
- `vehicle_positions_clean`  
- `trip_updates`  
- `alerts`  

### 🔥 Processing & Analytics

- Apache Spark / **PySpark** (Structured Streaming + batch pour la modélisation Bronze → Silver → Gold)  
- **pandas** (exploration, prototypes, tests, traitements légers)

### 🐍 Programming Language

- Python (langage principal sur toute la plateforme)

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
- Writes structured results to **Databricks** (Delta Lake)  

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
- Spark job execution (sur Databricks)  
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
├── spark/   (ou jobs Databricks)
│   ├── stream_processor.py
│   ├── schemas.py
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── airflow/
│   └── dags/
│       ├── ingestion_dag.py
│       ├── processing_dag.py
│       └── databricks_dag.py
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🚀 Project Phases

### Phase 1 – Local Setup

- Kafka (Docker)  
- Spark (Docker) ou environnement local PySpark  
- Airflow (Docker)  
- **pandas** pour l’exploration et les prototypes  
- Working Python producer  

### Phase 2 – Streaming Pipeline

- Reliable Spark streaming job  
- Schema management  
- Error handling  
- Writing to **Databricks** (Delta Lake)  

### Phase 3 – Cloud Deployment

- **Databricks** sur Azure  
- Secure connections  
- Deployment strategy  

### Phase 4 – Analytics Layer (PySpark)

- Modèles PySpark Bronze → Silver → Gold (sur Databricks)  
- Tests  
- Documentation  
- Data quality checks  
- **pandas** pour les tests et l’analyse exploratoire  

### Phase 5 – Advanced

- Great Expectations  
- CI/CD  
- Monitoring  
- ML forecasting  
- Infrastructure automation (Terraform optional)  

