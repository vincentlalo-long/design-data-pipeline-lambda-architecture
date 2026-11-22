import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from config import DATA_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC, RUNTIME_DIR


def parse_phases(phases_str: str) -> list[tuple[float, float]]:
    phases = []
    for part in phases_str.split(","):
        rate_s, dur_s = part.strip().split(":")
        phases.append((float(rate_s), float(dur_s)))
    return phases


def load_event_pool(parquet_path: Path, sample_jsonl: Path) -> list[dict]:
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
        return df.to_dict(orient="records")
    records = []
    with sample_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka replay producer with configurable rate phases")
    parser.add_argument("--phases", required=True, help="Comma-separated rate:duration_sec, e.g. '20:120' or '20:40,150:40,20:40'")
    parser.add_argument("--warmup-sec", type=float, default=20.0, help="Warm-up window excluded from sample freshness selection")
    parser.add_argument("--sample-target", type=int, default=120, help="Target number of sample events for freshness probing")
    parser.add_argument("--out-dir", type=Path, default=RUNTIME_DIR, help="Directory for emitted event logs")
    args = parser.parse_args()

    phases = parse_phases(args.phases)
    total_duration = sum(d for _, d in phases)
    expected_total = int(round(sum(r * d for r, d in phases)))
    # Estimate how many events fall after warm-up to evenly space >= 50 freshness samples
    post_warmup_est = max(1, int(expected_total * max(0.2, (total_duration - args.warmup_sec) / max(1.0, total_duration))))
    sample_stride = max(1, post_warmup_est // args.sample_target)

    pool = load_event_pool(DATA_DIR / "normalized_events.parquet", DATA_DIR / "sample_events.jsonl")
    if not pool:
        raise RuntimeError("No normalized events available to replay")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    emitted_csv = args.out_dir / "emitted_events.csv"
    progress_csv = args.out_dir / "producer_progress.csv"
    summary_json = args.out_dir / "producer_summary.json"

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=1,
        linger_ms=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    send_errors = 0

    def on_send_error(exc: Exception) -> None:
        nonlocal send_errors
        send_errors += 1
        print(f"[producer] send error: {exc}", file=sys.stderr)

    run_start = time.time()
    perf_start = time.perf_counter()
    seq = 0
    pool_idx = 0
    last_progress_t = perf_start

    with emitted_csv.open("w", newline="", encoding="utf-8") as ef, progress_csv.open(
        "w", newline="", encoding="utf-8"
    ) as pf:
        e_writer = csv.writer(ef)
        e_writer.writerow(["seq", "event_id", "category", "amount_cents", "event_time", "produced_at", "produced_epoch", "is_sample"])
        p_writer = csv.writer(pf)
        p_writer.writerow(["ts_epoch", "elapsed_s", "emitted_count", "send_errors", "target_rate"])
        p_writer.writerow([f"{run_start:.6f}", "0.000", 0, 0, phases[0][0]])
        pf.flush()

        phase_offset_s = 0.0
        for rate, dur_s in phases:
            if rate <= 0:
                time.sleep(dur_s)
                phase_offset_s += dur_s
                continue
            interval = 1.0 / rate
            phase_events = int(round(rate * dur_s))
            for i in range(phase_events):
                target_perf = perf_start + phase_offset_s + i * interval
                now_perf = time.perf_counter()
                if target_perf > now_perf:
                    time.sleep(target_perf - now_perf)

                base = pool[pool_idx % len(pool)]
                cycle = pool_idx // len(pool)
                pool_idx += 1
                seq += 1

                event_id = base["event_id"] if cycle == 0 else f"{base['event_id']}_c{cycle}_{seq}"
                now_utc = datetime.now(timezone.utc)
                now_epoch = now_utc.timestamp()
                produced_at = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                payload = {
                    "event_id": event_id,
                    "seq": seq,
                    "event_time": base["event_time"],
                    "produced_at": produced_at,
                    "category": base["category"],
                    "amount_cents": int(base["amount_cents"]),
                }

                fut = producer.send(KAFKA_TOPIC, key=base["category"], value=payload)
                fut.add_errback(on_send_error)

                elapsed_s = time.perf_counter() - perf_start
                is_sample = 1 if (elapsed_s >= args.warmup_sec and (seq % sample_stride == 0)) else 0
                e_writer.writerow([
                    seq,
                    event_id,
                    base["category"],
                    int(base["amount_cents"]),
                    base["event_time"],
                    produced_at,
                    f"{now_epoch:.6f}",
                    is_sample,
                ])

                now_p = time.perf_counter()
                if now_p - last_progress_t >= 0.5:
                    ef.flush()
                    p_writer.writerow([f"{time.time():.6f}", f"{elapsed_s:.3f}", seq, send_errors, rate])
                    pf.flush()
                    last_progress_t = now_p

            phase_offset_s += dur_s

        producer.flush()
        end_epoch = time.time()
        total_elapsed = time.perf_counter() - perf_start
        ef.flush()
        p_writer.writerow([f"{end_epoch:.6f}", f"{total_elapsed:.3f}", seq, send_errors, phases[-1][0]])
        pf.flush()

    producer.close()
    summary = {
        "phases": args.phases,
        "run_start_epoch": run_start,
        "run_end_epoch": end_epoch,
        "elapsed_s": round(total_elapsed, 4),
        "emitted_events": seq,
        "send_errors": send_errors,
        "actual_avg_rate": round(seq / max(1e-6, total_elapsed), 3),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[producer] done: {json.dumps(summary)}")


if __name__ == "__main__":
    main()
