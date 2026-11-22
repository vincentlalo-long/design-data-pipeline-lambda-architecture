import argparse
import csv
import signal
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from config import (
    CHECKPOINT_DIR,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    PG_DB,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    RUNTIME_DIR,
    SPARK_PACKAGES,
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("seq", LongType(), False),
    StructField("event_time", StringType(), False),
    StructField("produced_at", StringType(), False),
    StructField("category", StringType(), False),
    StructField("amount_cents", LongType(), False),
])

RUNNING = True


def handle_sig(signum, frame):
    global RUNNING
    RUNNING = False


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Speed Layer")
    parser.add_argument("--cores", type=int, default=2, help="Spark local[cores]")
    parser.add_argument("--memory", default="2g", help="Spark driver memory, e.g. 2g or 4g")
    parser.add_argument("--trigger-ms", type=int, default=2000, help="ProcessingTime trigger interval in ms")
    parser.add_argument("--shuffle-partitions", type=int, default=200, help="spark.sql.shuffle.partitions")
    parser.add_argument("--max-offsets", type=int, default=0, help="maxOffsetsPerTrigger (0 = unlimited)")
    parser.add_argument("--write-mode", choices=["row", "batch"], default="batch", help="PostgreSQL upsert mode")
    parser.add_argument("--checkpoint", type=Path, default=CHECKPOINT_DIR)
    parser.add_argument("--metrics-out", type=Path, default=RUNTIME_DIR / "speed_metrics.csv")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    args.checkpoint.mkdir(parents=True, exist_ok=True)
    args.metrics_out.parent.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("HW4-Lambda-SpeedLayer")
        .master(f"local[{args.cores}]")
        .config("spark.driver.memory", args.memory)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
    )
    if args.max_offsets > 0:
        kafka_reader = kafka_reader.option("maxOffsetsPerTrigger", str(args.max_offsets))

    raw_stream = kafka_reader.load()
    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("event_id").isNotNull())
    )

    write_header = not args.metrics_out.exists()
    metrics_file = args.metrics_out.open("a", newline="", encoding="utf-8")
    writer = csv.writer(metrics_file)
    if write_header:
        writer.writerow(["batch_id", "ts_epoch", "num_rows", "spark_ms", "pg_write_ms", "total_ms"])
        metrics_file.flush()

    def process_microbatch(batch_df, batch_id: int) -> None:
        t0 = time.perf_counter()
        dedup_df = batch_df.dropDuplicates(["event_id"])
        rows = dedup_df.collect()
        t_spark = time.perf_counter()
        spark_ms = (t_spark - t0) * 1000.0

        if not rows:
            return

        records = [
            (
                r["event_id"],
                int(r["seq"]),
                r["category"],
                int(r["amount_cents"]),
                r["produced_at"],
            )
            for r in rows
        ]

        t_pg0 = time.perf_counter()
        conn = get_pg_conn()
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                if args.write_mode == "row":
                    for rec in records:
                        cur.execute(
                            """
                            INSERT INTO speed_events (event_id, seq, category, amount_cents, produced_at)
                            VALUES (%s, %s, %s, %s, %s::timestamptz)
                            ON CONFLICT (event_id) DO NOTHING;
                            """,
                            rec,
                        )
                else:
                    execute_values(
                        cur,
                        """
                        INSERT INTO speed_events (event_id, seq, category, amount_cents, produced_at)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING;
                        """,
                        records,
                        template="(%s, %s, %s, %s, %s::timestamptz)",
                        page_size=500,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        t_pg1 = time.perf_counter()
        pg_ms = (t_pg1 - t_pg0) * 1000.0
        total_ms = (t_pg1 - t0) * 1000.0

        writer.writerow([batch_id, f"{time.time():.6f}", len(records), f"{spark_ms:.3f}", f"{pg_ms:.3f}", f"{total_ms:.3f}"])
        metrics_file.flush()

    query = (
        parsed.writeStream.foreachBatch(process_microbatch)
        .option("checkpointLocation", str(args.checkpoint))
        .trigger(processingTime=f"{args.trigger_ms} milliseconds")
        .start()
    )
    print(f"[speed_layer] started (cores={args.cores}, mem={args.memory}, trigger={args.trigger_ms}ms, shuffle={args.shuffle_partitions}, mode={args.write_mode})")

    try:
        while RUNNING and query.isActive:
            query.awaitTermination(timeout=0.5)
    finally:
        if query.isActive:
            query.stop()
        metrics_file.close()
        spark.stop()
        print("[speed_layer] stopped")


if __name__ == "__main__":
    main()
