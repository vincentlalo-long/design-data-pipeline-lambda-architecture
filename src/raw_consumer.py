import argparse
import json
import os
import signal
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, RAW_MANIFEST_PATH, RAW_PARQUET_DIR

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("seq", pa.int64()),
    ("event_time", pa.string()),
    ("produced_at", pa.string()),
    ("category", pa.string()),
    ("amount_cents", pa.int64()),
])

RUNNING = True


def handle_sig(signum, frame):
    global RUNNING
    RUNNING = False


def atomic_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def flush_buffer(
    buf: list[dict],
    out_dir: Path,
    manifest_path: Path,
    committed_files: list[dict],
    seen_seqs: set[int],
    contiguous_h: int,
    total_written: int,
) -> tuple[int, int]:
    if not buf:
        return contiguous_h, total_written

    seqs = [int(r["seq"]) for r in buf]
    min_seq, max_seq = min(seqs), max(seqs)
    part_name = f"part_{min_seq:08d}_{max_seq:08d}_{int(time.time() * 1000)}.parquet"
    final_path = out_dir / part_name
    tmp_path = out_dir / f"{part_name}.tmp"

    table = pa.Table.from_pylist(buf, schema=SCHEMA)
    pq.write_table(table, tmp_path, compression="snappy")
    os.replace(tmp_path, final_path)

    for s in seqs:
        seen_seqs.add(s)
    while (contiguous_h + 1) in seen_seqs:
        contiguous_h += 1

    total_written += len(buf)
    committed_files.append({
        "file": str(final_path),
        "min_seq": min_seq,
        "max_seq": max_seq,
        "rows": len(buf),
    })

    atomic_write_json(
        manifest_path,
        {
            "contiguous_watermark_seq": contiguous_h,
            "total_records": total_written,
            "file_count": len(committed_files),
            "updated_at_epoch": time.time(),
            "files": committed_files,
        },
    )
    return contiguous_h, total_written


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka consumer writing immutable Parquet files with atomic manifest")
    parser.add_argument("--flush-interval", type=float, default=1.0, help="Max seconds between Parquet flushes")
    parser.add_argument("--flush-max-rows", type=int, default=500, help="Max rows buffered before flush")
    parser.add_argument("--out-dir", type=Path, default=RAW_PARQUET_DIR)
    parser.add_argument("--manifest", type=Path, default=RAW_MANIFEST_PATH)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="hw4-raw-storage-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=250,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    committed_files: list[dict] = []
    seen_seqs: set[int] = set()
    contiguous_h = 0
    total_written = 0
    buf: list[dict] = []
    last_flush = time.monotonic()

    atomic_write_json(
        args.manifest,
        {
            "contiguous_watermark_seq": 0,
            "total_records": 0,
            "file_count": 0,
            "updated_at_epoch": time.time(),
            "files": [],
        },
    )
    print("[raw_consumer] started, listening on Kafka...")

    try:
        while RUNNING:
            polled = consumer.poll(timeout_ms=250, max_records=500)
            for _, records in polled.items():
                for msg in records:
                    v = msg.value
                    buf.append({
                        "event_id": str(v["event_id"]),
                        "seq": int(v["seq"]),
                        "event_time": str(v["event_time"]),
                        "produced_at": str(v["produced_at"]),
                        "category": str(v["category"]),
                        "amount_cents": int(v["amount_cents"]),
                    })

            now = time.monotonic()
            if buf and (len(buf) >= args.flush_max_rows or (now - last_flush) >= args.flush_interval):
                contiguous_h, total_written = flush_buffer(
                    buf, args.out_dir, args.manifest, committed_files, seen_seqs, contiguous_h, total_written
                )
                buf.clear()
                last_flush = now
    finally:
        # Drain remaining messages in Kafka before shutdown
        polled = consumer.poll(timeout_ms=300, max_records=2000)
        for _, records in polled.items():
            for msg in records:
                v = msg.value
                buf.append({
                    "event_id": str(v["event_id"]),
                    "seq": int(v["seq"]),
                    "event_time": str(v["event_time"]),
                    "produced_at": str(v["produced_at"]),
                    "category": str(v["category"]),
                    "amount_cents": int(v["amount_cents"]),
                })
        if buf:
            contiguous_h, total_written = flush_buffer(
                buf, args.out_dir, args.manifest, committed_files, seen_seqs, contiguous_h, total_written
            )
        consumer.close()
        print(f"[raw_consumer] stopped. contiguous_H={contiguous_h}, total_written={total_written}")


if __name__ == "__main__":
    main()
