import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
RUNTIME_DIR = BASE_DIR / "runtime"
RAW_PARQUET_DIR = RUNTIME_DIR / "raw_parquet"
RAW_MANIFEST_PATH = RUNTIME_DIR / "raw_manifest.json"
CHECKPOINT_DIR = RUNTIME_DIR / "spark_checkpoint"
METRICS_DIR = BASE_DIR / "metrics"
LOGS_DIR = BASE_DIR / "logs"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc_taxi_events")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "lambda_serving")
PG_USER = os.getenv("PG_USER", "lambda")
PG_PASSWORD = os.getenv("PG_PASSWORD", "lambda_pass")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
SPARK_PG_PACKAGE = "org.postgresql:postgresql:42.7.4"
SPARK_PACKAGES = f"{SPARK_KAFKA_PACKAGE},{SPARK_PG_PACKAGE}"
