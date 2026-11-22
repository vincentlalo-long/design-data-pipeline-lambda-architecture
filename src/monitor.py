import argparse
import bisect
import csv
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
import psycopg2

from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER, RUNTIME_DIR


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


class ProcessTracker:
    """Keep Process instances alive across polls so cpu_percent(None) tracks real deltas."""

    TARGET_MARKERS = ("raw_consumer.py", "batch_layer.py", "speed_layer.py")

    def __init__(self, root_pids: list[int]) -> None:
        self.root_pids = root_pids
        self.procs: dict[int, psutil.Process] = {}
        psutil.cpu_percent(interval=None)
        self._refresh()

    def _refresh(self) -> None:
        alive_pids = set()
        roots = list(self.root_pids)
        for p in psutil.process_iter(["pid", "cmdline"]):
            try:
                cmd = " ".join(p.info.get("cmdline") or [])
                if any(m in cmd for m in self.TARGET_MARKERS):
                    roots.append(p.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        for pid in set(roots):
            try:
                if pid not in self.procs:
                    self.procs[pid] = psutil.Process(pid)
                    self.procs[pid].cpu_percent(interval=None)
                root = self.procs[pid]
                alive_pids.add(pid)
                for child in root.children(recursive=True):
                    alive_pids.add(child.pid)
                    if child.pid not in self.procs:
                        self.procs[child.pid] = child
                        child.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        for dead in [p for p in self.procs if p not in alive_pids]:
            del self.procs[dead]

    def sample(self) -> tuple[float, float]:
        self._refresh()
        total_cpu = 0.0
        total_rss = 0
        for p in list(self.procs.values()):
            try:
                total_cpu += p.cpu_percent(interval=None)
                total_rss += p.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return total_cpu, total_rss / (1024.0 * 1024.0)


def load_known_samples(emitted_csv: Path, seen_seqs: set[int]) -> list[dict]:
    if not emitted_csv.exists():
        return []
    new_samples = []
    with emitted_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("is_sample") == "1":
                s = int(row["seq"])
                if s not in seen_seqs:
                    seen_seqs.add(s)
                    new_samples.append({
                        "seq": s,
                        "event_id": row["event_id"],
                        "produced_epoch": float(row["produced_epoch"]),
                    })
    return new_samples


def verify_correctness(emitted_csv: Path) -> dict:
    df = pd.read_csv(emitted_csv)
    df_unique = df.drop_duplicates(subset=["event_id"])
    gt_total_count = int(len(df_unique))
    gt_total_sum = int(df_unique["amount_cents"].sum())
    gt_by_cat = (
        df_unique.groupby("category")["amount_cents"]
        .agg(["count", "sum"])
        .to_dict(orient="index")
    )

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(event_count), 0)::BIGINT,
                       COALESCE(SUM(total_amount_cents), 0)::BIGINT
                FROM serving_category_agg;
                """
            )
            pg_total_count, pg_total_sum = cur.fetchone()
            pg_total_count = int(pg_total_count)
            pg_total_sum = int(pg_total_sum)

            cur.execute("SELECT category, event_count, total_amount_cents FROM serving_category_agg;")
            pg_by_cat = {r[0]: {"count": int(r[1]), "sum": int(r[2])} for r in cur.fetchall()}
    finally:
        conn.close()

    mismatches = 0
    all_cats = set(gt_by_cat.keys()) | set(pg_by_cat.keys())
    for cat in all_cats:
        g = gt_by_cat.get(cat, {"count": 0, "sum": 0})
        p = pg_by_cat.get(cat, {"count": 0, "sum": 0})
        if int(g["count"]) != int(p["count"]) or int(g["sum"]) != int(p["sum"]):
            mismatches += 1

    return {
        "gt_total_count": gt_total_count,
        "pg_total_count": pg_total_count,
        "count_diff": pg_total_count - gt_total_count,
        "gt_total_sum_cents": gt_total_sum,
        "pg_total_sum_cents": pg_total_sum,
        "sum_diff_cents": pg_total_sum - gt_total_sum,
        "category_mismatches": mismatches,
        "correct": (pg_total_count == gt_total_count and pg_total_sum == gt_total_sum and mismatches == 0),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Independent PostgreSQL serving observer & resource monitor")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--poll-ms", type=int, default=200, help="Serving poll period in ms")
    parser.add_argument("--drain-timeout", type=float, default=60.0, help="Max seconds to wait for catch-up after producer finishes")
    parser.add_argument("--pids", default="", help="Comma-separated PIDs of pipeline processes to monitor")
    parser.add_argument("--runtime-dir", type=Path, default=RUNTIME_DIR)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    pids = [int(p) for p in args.pids.split(",") if p.strip().isdigit()]
    tracker = ProcessTracker(pids)

    emitted_csv = args.runtime_dir / "emitted_events.csv"
    summary_json = args.runtime_dir / "producer_summary.json"
    args.out_dir.mkdir(parents=True, exist_ok=True)

    poll_sec = args.poll_ms / 1000.0
    seen_sample_seqs: set[int] = set()
    pending_samples: dict[str, dict] = {}
    resolved_samples: list[dict] = []
    timeseries: list[dict] = []
    q1_latencies_ms: list[float] = []
    q2_latencies_ms: list[float] = []

    conn = get_pg_conn()
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=True)

    t0 = time.monotonic()
    producer_done_mono = None
    target_events = None
    last_resource_t = t0
    last_sql_bench_t = t0
    last_sample_scan_t = t0
    last_sys_cpu = 0.0
    last_app_cpu = 0.0
    last_app_ram = 0.0

    try:
        while True:
            loop_start = time.monotonic()
            now_epoch = time.time()

            if summary_json.exists() and producer_done_mono is None:
                prod_sum = json.loads(summary_json.read_text(encoding="utf-8"))
                target_events = int(prod_sum["emitted_events"])
                producer_done_mono = loop_start

            # Periodically refresh sample list from emitted_events.csv
            if loop_start - last_sample_scan_t >= 0.5:
                for smp in load_known_samples(emitted_csv, seen_sample_seqs):
                    pending_samples[smp["event_id"]] = smp
                last_sample_scan_t = loop_start

            sample_ids = list(pending_samples.keys())
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH wm AS (
                        SELECT batch_watermark_seq AS h FROM pipeline_state WHERE id = 1
                    ),
                    q2 AS (
                        SELECT
                            COALESCE(SUM(event_count), 0)::BIGINT AS total_count,
                            COALESCE(SUM(total_amount_cents), 0)::BIGINT AS total_amount_cents
                        FROM serving_category_agg
                    )
                    SELECT
                        (SELECT h FROM wm) AS watermark_h,
                        q2.total_count,
                        q2.total_amount_cents,
                        ARRAY(
                            SELECT s.event_id
                            FROM speed_events s, wm
                            WHERE s.event_id = ANY(%s) AND s.seq > wm.h
                        ) AS visible_speed_ids
                    FROM q2;
                    """,
                    (sample_ids,),
                )
                wm_h, vis_count, vis_sum, vis_speed_ids = cur.fetchone()
                wm_h = int(wm_h)
                vis_count = int(vis_count)
                vis_speed_set = set(vis_speed_ids or [])

                # Measure Q1/Q2 serving SQL latency every 2 seconds
                q1_ms = None
                q2_ms = None
                if loop_start - last_sql_bench_t >= 2.0:
                    qs0 = time.perf_counter()
                    cur.execute("SELECT category, event_count, total_amount_cents FROM serving_category_agg ORDER BY total_amount_cents DESC;")
                    cur.fetchall()
                    q1_ms = (time.perf_counter() - qs0) * 1000.0
                    q1_latencies_ms.append(q1_ms)

                    qs1 = time.perf_counter()
                    cur.execute("SELECT COALESCE(SUM(event_count),0), COALESCE(SUM(total_amount_cents),0) FROM serving_category_agg;")
                    cur.fetchone()
                    q2_ms = (time.perf_counter() - qs1) * 1000.0
                    q2_latencies_ms.append(q2_ms)
                    last_sql_bench_t = loop_start

            # Mark any pending sample events now confirmed visible in serving view
            newly_resolved = []
            for eid, smp in pending_samples.items():
                if smp["seq"] <= wm_h:
                    via = "batch"
                elif eid in vis_speed_set:
                    via = "speed"
                else:
                    continue
                freshness_ms = max(0.0, (now_epoch - smp["produced_epoch"]) * 1000.0)
                resolved_samples.append({
                    "seq": smp["seq"],
                    "event_id": eid,
                    "produced_epoch": smp["produced_epoch"],
                    "visible_epoch": now_epoch,
                    "freshness_ms": round(freshness_ms, 3),
                    "visible_via": via,
                    "appeared_during_run": 1 if producer_done_mono is None else 0,
                })
                newly_resolved.append(eid)
            for eid in newly_resolved:
                del pending_samples[eid]

            if loop_start - last_resource_t >= 1.0:
                last_sys_cpu = psutil.cpu_percent(interval=None)
                last_app_cpu, last_app_ram = tracker.sample()
                last_resource_t = loop_start

            timeseries.append({
                "ts_epoch": now_epoch,
                "elapsed_s": round(loop_start - t0, 3),
                "watermark_h": wm_h,
                "visible_count": vis_count,
                "sys_cpu_pct": round(last_sys_cpu, 2),
                "app_cpu_pct": round(last_app_cpu, 2),
                "app_ram_mb": round(last_app_ram, 2),
                "q1_ms": round(q1_ms, 3) if q1_ms is not None else "",
                "q2_ms": round(q2_ms, 3) if q2_ms is not None else "",
            })

            if producer_done_mono is not None:
                # Final scan to catch tail samples before exit check
                for smp in load_known_samples(emitted_csv, seen_sample_seqs):
                    pending_samples[smp["event_id"]] = smp
                if vis_count >= target_events and not pending_samples:
                    break
                if (loop_start - producer_done_mono) >= args.drain_timeout:
                    break

            elapsed_loop = time.monotonic() - loop_start
            if elapsed_loop < poll_sec:
                time.sleep(poll_sec - elapsed_loop)
    finally:
        conn.close()

    # Reconcile exact emitted_count at each ts_epoch using emitted_events.csv
    emitted_df = pd.read_csv(emitted_csv)
    produced_epochs = np.sort(emitted_df["produced_epoch"].to_numpy(dtype=float))
    run_start_epoch = float(produced_epochs[0]) if len(produced_epochs) > 0 else timeseries[0]["ts_epoch"]

    for idx, row in enumerate(timeseries):
        emitted_at_t = int(bisect.bisect_right(produced_epochs, row["ts_epoch"]))
        row["rel_s"] = round(row["ts_epoch"] - run_start_epoch, 3)
        row["emitted_count"] = emitted_at_t
        row["backlog"] = max(0, emitted_at_t - row["visible_count"])
        if idx == 0:
            row["throughput_eps"] = 0.0
        else:
            dt = max(1e-6, row["ts_epoch"] - timeseries[idx - 1]["ts_epoch"])
            dv = row["visible_count"] - timeseries[idx - 1]["visible_count"]
            row["throughput_eps"] = round(max(0.0, dv / dt), 3)

    # Mark unresolved samples if drain timed out
    for eid, smp in pending_samples.items():
        resolved_samples.append({
            "seq": smp["seq"],
            "event_id": eid,
            "produced_epoch": smp["produced_epoch"],
            "visible_epoch": "",
            "freshness_ms": "",
            "visible_via": "unseen",
            "appeared_during_run": 0,
        })

    ts_df = pd.DataFrame(timeseries)
    ts_path = args.out_dir / f"{args.run_id}_timeseries.csv"
    ts_df.to_csv(ts_path, index=False)

    samp_df = pd.DataFrame(resolved_samples).sort_values("seq") if resolved_samples else pd.DataFrame()
    samp_path = args.out_dir / f"{args.run_id}_freshness.csv"
    samp_df.to_csv(samp_path, index=False)

    # Compute summary metrics (steady-state excludes initial 20s warm-up for throughput/resource averages)
    prod_sum = json.loads(summary_json.read_text(encoding="utf-8"))
    run_end_epoch = float(prod_sum["run_end_epoch"])
    steady = ts_df[(ts_df["ts_epoch"] >= run_start_epoch + 20.0) & (ts_df["ts_epoch"] <= run_end_epoch)]
    if steady.empty:
        steady = ts_df[ts_df["ts_epoch"] <= run_end_epoch]

    valid_fresh = samp_df[samp_df["visible_via"] != "unseen"]["freshness_ms"].astype(float) if not samp_df.empty else pd.Series(dtype=float)
    unseen_during_run = int((samp_df["appeared_during_run"] == 0).sum()) if not samp_df.empty else 0
    unseen_after_drain = int((samp_df["visible_via"] == "unseen").sum()) if not samp_df.empty else 0

    steady_dur = max(1e-6, float(steady["ts_epoch"].iloc[-1] - steady["ts_epoch"].iloc[0])) if len(steady) > 1 else 1.0
    steady_vis_delta = int(steady["visible_count"].iloc[-1] - steady["visible_count"].iloc[0]) if len(steady) > 1 else 0
    steady_throughput = steady_vis_delta / steady_dur
    drain_end_epoch = float(ts_df["ts_epoch"].iloc[-1]) if not ts_df.empty else run_end_epoch
    total_drain_dur = max(1e-6, drain_end_epoch - run_start_epoch)
    overall_drain_throughput = int(prod_sum["emitted_events"]) / total_drain_dur

    correctness = verify_correctness(emitted_csv)

    summary = {
        "run_id": args.run_id,
        "poll_ms": args.poll_ms,
        "emitted_events": int(prod_sum["emitted_events"]),
        "actual_input_rate_eps": float(prod_sum["actual_avg_rate"]),
        "send_errors": int(prod_sum["send_errors"]),
        "steady_throughput_eps": round(steady_throughput, 3),
        "overall_drain_throughput_eps": round(overall_drain_throughput, 3),
        "total_drain_duration_s": round(total_drain_dur, 2),
        "freshness_sample_count": int(len(samp_df)),
        "unseen_during_emission": unseen_during_run,
        "unseen_after_drain": unseen_after_drain,
        "freshness_p50_ms": round(float(np.percentile(valid_fresh, 50)), 2) if len(valid_fresh) else None,
        "freshness_p95_ms": round(float(np.percentile(valid_fresh, 95)), 2) if len(valid_fresh) else None,
        "freshness_max_ms": round(float(valid_fresh.max()), 2) if len(valid_fresh) else None,
        "avg_backlog_steady": round(float(steady["backlog"].mean()), 2) if not steady.empty else 0.0,
        "max_backlog": int(ts_df["backlog"].max()) if not ts_df.empty else 0,
        "avg_app_cpu_pct": round(float(steady["app_cpu_pct"].mean()), 2) if not steady.empty else 0.0,
        "peak_app_cpu_pct": round(float(ts_df["app_cpu_pct"].max()), 2) if not ts_df.empty else 0.0,
        "avg_app_ram_mb": round(float(steady["app_ram_mb"].mean()), 2) if not steady.empty else 0.0,
        "peak_app_ram_mb": round(float(ts_df["app_ram_mb"].max()), 2) if not ts_df.empty else 0.0,
        "avg_sys_cpu_pct": round(float(steady["sys_cpu_pct"].mean()), 2) if not steady.empty else 0.0,
        "peak_sys_cpu_pct": round(float(ts_df["sys_cpu_pct"].max()), 2) if not ts_df.empty else 0.0,
        "q1_p95_ms": round(float(np.percentile(q1_latencies_ms, 95)), 2) if q1_latencies_ms else None,
        "q2_p95_ms": round(float(np.percentile(q2_latencies_ms, 95)), 2) if q2_latencies_ms else None,
        "correctness": correctness,
    }

    sum_path = args.out_dir / f"{args.run_id}_summary.json"
    sum_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[monitor] {args.run_id} summary: {json.dumps(summary)}")


if __name__ == "__main__":
    main()
