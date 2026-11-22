# IT4931 Assignment 4 — Lambda Architecture Pipeline & Performance Evaluation

Experimental comparison of **Option A (Batch-only)** vs. **Option B (Lambda: Batch + Speed)** on real **NYC TLC Yellow Taxi Trip Records (2024-01)** using Apache Kafka (KRaft), Apache Spark (PySpark), Parquet raw storage, and PostgreSQL 16 serving layer.

---

## 1. Repository Layout

```text
├── docker-compose.yml          # Kafka (KRaft, 1 partition default) + PostgreSQL 16
├── requirements.txt            # Python dependencies
├── data/
│   ├── data_manifest.json      # Source URL, SHA-256, cleaning counts, seed=42
│   └── sample_events.jsonl     # 1,500 normalized sample events (>= 1,000 required)
├── sql/
│   ├── schema.sql              # pipeline_state, batch_category_agg, speed_events, serving_category_agg
│   └── queries_q1_q2.sql       # Q1 (per-category), Q2 (system total), diagnostic check
├── src/
│   ├── config.py               # Shared paths, Kafka/PG/Spark settings
│   ├── prepare_data.py         # Download NYC TLC parquet, clean, normalize 50k pool + 1.5k sample
│   ├── producer.py             # Configurable-rate Kafka replay (seq=1..N, UTC produced_at)
│   ├── raw_consumer.py         # Immutable Snappy Parquet writer + atomic raw_manifest.json
│   ├── batch_layer.py          # Periodic Spark Batch (30s) with atomic PG handoff & speed pruning
│   ├── speed_layer.py          # Spark Structured Streaming + checkpoint + idempotent PG upsert
│   ├── monitor.py              # 200ms MVCC serving observer (freshness, backlog, CPU/RAM, Q1/Q2)
│   ├── verify_queries.py       # CLI tool to verify Q1, Q2, and ground-truth reconciliation
│   ├── run_experiments.py      # Orchestrator for E1–E5 with clean state resets
│   ├── analyze_metrics.py      # Builds summary_runs.csv and prints batch/speed breakdowns
│   ├── make_diagram.py         # Generates report/architecture_diagram.png
│   └── plot_charts.py          # Generates summary_aggregated.csv and Charts 1–4
├── metrics/
│   ├── summary_runs.csv        # All 15 individual run summaries (E1–E5)
│   ├── summary_aggregated.csv  # Aggregated means by experimental condition
│   ├── verification_proof.txt  # Captured live Q1/Q2 & reconciliation output
│   ├── raw/                    # Per-run timeseries, freshness, batch/speed CSVs, summaries
│   └── charts/                 # chart1..chart4 PNGs
└── report/
    ├── architecture_diagram.png
    ├── generate_report_pdf.py
    ├── HW4_Report.md
    └── HW4_Report.pdf          # Final 5-page report
```

---

## 2. Environment & Tool Versions

- **Host OS / Runtime**: Windows 11 (WSL2 Ubuntu)
- **Hardware**: AMD Ryzen 7 7735HS (8 Cores / 16 Threads), 15.24 GB host RAM (7.4 GiB allocated to WSL2)
- **Docker**: Docker Engine v29.8.0, Docker Compose v5.5.1
- **Apache Kafka**: `apache/kafka:latest` (KRaft mode, 1 topic `nyc_taxi_events`, 1 partition)
- **PostgreSQL**: `postgres:16-alpine`
- **Apache Spark**: PySpark `4.3.0` (Local mode, OpenJDK `17.0.20.1`, `spark-sql-kafka-0-10_2.13:4.3.0`, `postgresql:42.7.4`)
- **Python**: `3.14.4` (WSL2)
- **Spark Resource Limits**:
  - **Baseline / E1–E4 (`C1`)**: `local[2]`, `spark.driver.memory=2g`
  - **Scale-up (`C2`, E5)**: `local[4]`, `spark.driver.memory=4g`

---

## 3. Setup & Data Preparation

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Start Kafka and PostgreSQL
docker compose up -d

# 3. Download, clean, and normalize NYC TLC Yellow Taxi 2024-01 data (50,000 events, seed=42)
python3 src/prepare_data.py

# 4. Pre-cache Spark Kafka/PostgreSQL Maven jars
PYTHONPATH=src python3 src/run_experiments.py --prewarm
```

Cleaning rules & exact row counts are recorded in [data/data_manifest.json](data/data_manifest.json).

---

## 4. State Reset & Running Option A / Option B Manually

Always reset Kafka, PostgreSQL, Parquet raw files, and Spark checkpoints before a new run:

```bash
PYTHONPATH=src python3 src/run_experiments.py --reset
```

### Option A — Batch-only (30s periodic Spark Batch)
```bash
PYTHONPATH=src python3 src/raw_consumer.py &
PYTHONPATH=src python3 src/batch_layer.py --cores 2 --memory 2g --interval 30 &
PYTHONPATH=src python3 src/producer.py --phases 100:120
```

### Option B — Lambda (Batch + Speed Layer)
```bash
PYTHONPATH=src python3 src/raw_consumer.py &
PYTHONPATH=src python3 src/batch_layer.py --cores 2 --memory 2g --interval 30 &
PYTHONPATH=src python3 src/speed_layer.py --cores 2 --memory 2g --trigger-ms 500 --shuffle-partitions 4 &
PYTHONPATH=src python3 src/producer.py --phases 100:120
```

---

## 5. Reproducing Experiments E1–E5 & Verification

```bash
# Run baseline suite (E1: 20 eps, E2: 100 eps, E3: 30s speed-layer fault + checkpoint restart)
PYTHONPATH=src python3 src/run_experiments.py --suite baseline

# Run E4 (tuned micro-batch trigger=500ms, shuffle.partitions=4) and E5 (C1 2c/2g vs C2 4c/4g @ 1000 eps)
PYTHONPATH=src python3 src/run_experiments.py --suite e4
PYTHONPATH=src python3 src/run_experiments.py --suite e5

# Aggregate metrics and regenerate diagram + 4 charts
PYTHONPATH=src python3 src/analyze_metrics.py
PYTHONPATH=src python3 src/make_diagram.py
PYTHONPATH=src python3 src/plot_charts.py

# Verify Q1, Q2, and ground-truth COUNT/SUM reconciliation
PYTHONPATH=src python3 src/verify_queries.py
docker exec -i hw4-postgres psql -U lambda -d lambda_serving < sql/queries_q1_q2.sql
```
