import glob
import json
from pathlib import Path
import pandas as pd

from config import METRICS_DIR


def main() -> None:
    raw_dir = METRICS_DIR / "raw"
    rows = []
    for p in sorted(raw_dir.glob("*_summary.json")):
        if p.name.endswith("_producer_summary.json"):
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        corr = d.get("correctness", {})
        rows.append({
            "run_id": d["run_id"],
            "emitted_events": d["emitted_events"],
            "input_rate_eps": d["actual_input_rate_eps"],
            "steady_throughput_eps": d["steady_throughput_eps"],
            "overall_drain_throughput_eps": d["overall_drain_throughput_eps"],
            "freshness_p50_ms": d["freshness_p50_ms"],
            "freshness_p95_ms": d["freshness_p95_ms"],
            "freshness_max_ms": d["freshness_max_ms"],
            "avg_backlog_steady": d["avg_backlog_steady"],
            "max_backlog": d["max_backlog"],
            "avg_app_cpu_pct": d["avg_app_cpu_pct"],
            "peak_app_cpu_pct": d["peak_app_cpu_pct"],
            "avg_app_ram_mb": d["avg_app_ram_mb"],
            "peak_app_ram_mb": d["peak_app_ram_mb"],
            "avg_sys_cpu_pct": d["avg_sys_cpu_pct"],
            "peak_sys_cpu_pct": d["peak_sys_cpu_pct"],
            "q1_p95_ms": d["q1_p95_ms"],
            "q2_p95_ms": d["q2_p95_ms"],
            "unseen_during_emission": d["unseen_during_emission"],
            "unseen_after_drain": d["unseen_after_drain"],
            "count_diff": corr.get("count_diff"),
            "sum_diff_cents": corr.get("sum_diff_cents"),
            "correct": corr.get("correct"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        print("=== SUMMARY TABLE ===")
        print(df.to_string(index=False))
        df.to_csv(METRICS_DIR / "summary_runs.csv", index=False)

    for sm_path in sorted(raw_dir.glob("*_speed_metrics.csv")):
        sm = pd.read_csv(sm_path)
        print(f"\n--- {sm_path.name} (batches={len(sm)}) ---")
        print(sm[["num_rows", "spark_ms", "pg_write_ms", "total_ms"]].describe().loc[["mean", "50%", "max"]].round(2).to_string())

    for bm_path in sorted(raw_dir.glob("*_batch_metrics.csv")):
        bm = pd.read_csv(bm_path)
        print(f"\n--- {bm_path.name} (runs={len(bm)}) ---")
        print(bm[["watermark_h", "batch_events", "duration_ms"]].to_string(index=False))


if __name__ == "__main__":
    main()
