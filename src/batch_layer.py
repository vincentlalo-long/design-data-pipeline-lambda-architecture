import argparse
import csv
import json
import signal
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER, RAW_MANIFEST_PATH, RUNTIME_DIR

RUNNING = True


def handle_sig(signum, frame):
    global RUNNING
    RUNNING = False


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def run_batch_cycle(spark: SparkSession, manifest_path: Path, run_idx: int) -> tuple[int, int, float]:
    t0 = time.perf_counter()
    if not manifest_path.exists():
        return 0, 0, 0.0

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    h_target = int(manifest.get("contiguous_watermark_seq", 0))
    if h_target <= 0:
        return 0, 0, 0.0

    # Read only immutable Parquet files whose min_seq <= h_target
    files = [f["file"] for f in manifest.get("files", []) if int(f["min_seq"]) <= h_target]
    if not files:
        return 0, 0, 0.0

    df = spark.read.parquet(*files).filter(F.col("seq") <= h_target).dropDuplicates(["event_id"])
    agg_rows = (
        df.groupBy("category")
        .agg(
            F.count("*").cast("bigint").alias("event_count"),
            F.sum("amount_cents").cast("bigint").alias("total_amount_cents"),
        )
        .collect()
    )
    payload = [(r["category"], int(r["event_count"]), int(r["total_amount_cents"])) for r in agg_rows]

    # Atomic PostgreSQL handoff: update batch snapshot, advance watermark H, and prune covered speed events
    conn = get_pg_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("LOCK TABLE pipeline_state IN EXCLUSIVE MODE;")
            cur.execute("DELETE FROM batch_category_agg;")
            if payload:
                execute_values(
                    cur,
                    "INSERT INTO batch_category_agg (category, event_count, total_amount_cents) VALUES %s",
                    payload,
                    page_size=500,
                )
            cur.execute(
                """
                UPDATE pipeline_state
                SET batch_watermark_seq = %s,
                    batch_runs = batch_runs + 1,
                    updated_at = NOW()
                WHERE id = 1;
                """,
                (h_target,),
            )
            cur.execute("DELETE FROM speed_events WHERE seq <= %s;", (h_target,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    dur_ms = (time.perf_counter() - t0) * 1000.0
    total_cnt = sum(r[1] for r in payload)
    print(f"[batch_layer] run={run_idx} H={h_target} categories={len(payload)} events={total_cnt} dur={dur_ms:.1f}ms")
    return h_target, total_cnt, dur_ms


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Batch layer updating PostgreSQL serving snapshot atomically")
    parser.add_argument("--cores", type=int, default=2, help="Spark local[cores]")
    parser.add_argument("--memory", default="2g", help="Spark driver memory, e.g. 2g or 4g")
    parser.add_argument("--interval", type=float, default=30.0, help="Batch period in seconds")
    parser.add_argument("--shuffle-partitions", type=int, default=4, help="spark.sql.shuffle.partitions")
    parser.add_argument("--once", action="store_true", help="Run one batch snapshot and exit")
    parser.add_argument("--manifest", type=Path, default=RAW_MANIFEST_PATH)
    parser.add_argument("--metrics-out", type=Path, default=RUNTIME_DIR / "batch_metrics.csv")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    spark = (
        SparkSession.builder.appName("HW4-Lambda-BatchLayer")
        .master(f"local[{args.cores}]")
        .config("spark.driver.memory", args.memory)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[batch_layer] started (cores={args.cores}, mem={args.memory}, interval={args.interval}s)", flush=True)

    args.metrics_out.parent.mkdir(parents=True, exist_ok=True)
    write_header = not args.metrics_out.exists()
    with args.metrics_out.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["run_idx", "ts_epoch", "watermark_h", "batch_events", "duration_ms"])

        run_idx = 0
        try:
            if args.once:
                run_idx = 1
                h, cnt, dur = run_batch_cycle(spark, args.manifest, run_idx)
                writer.writerow([run_idx, f"{time.time():.6f}", h, cnt, f"{dur:.3f}"])
                f.flush()
            else:
                # Wait full interval before first scheduled batch to reflect periodic 30s cadence
                next_tick = time.monotonic() + args.interval
                while RUNNING:
                    now = time.monotonic()
                    if now < next_tick:
                        time.sleep(min(0.5, next_tick - now))
                        continue
                    run_idx += 1
                    h, cnt, dur = run_batch_cycle(spark, args.manifest, run_idx)
                    writer.writerow([run_idx, f"{time.time():.6f}", h, cnt, f"{dur:.3f}"])
                    f.flush()
                    next_tick += args.interval
                    if next_tick < time.monotonic():
                        next_tick = time.monotonic() + args.interval
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
