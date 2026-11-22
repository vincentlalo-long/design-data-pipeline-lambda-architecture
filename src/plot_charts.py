import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from config import METRICS_DIR

plt.rcParams.update({
    "font.size": 9.5,
    "axes.titlesize": 10.5,
    "axes.titleweight": "bold",
    "axes.labelsize": 9.5,
    "legend.fontsize": 8.5,
    "figure.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linestyle": "--",
})


def build_aggregated_summary(runs_df: pd.DataFrame) -> pd.DataFrame:
    def map_group(run_id: str) -> str:
        if run_id.startswith("E1_batch"):
            return "E1_Batch_20eps"
        if run_id.startswith("E1_lambda"):
            return "E1_Lambda_20eps"
        if run_id.startswith("E2_batch"):
            return "E2_Batch_100eps"
        if run_id.startswith("E2_lambda"):
            return "E2_Lambda_100eps_Baseline"
        if run_id.startswith("E3_lambda_fault"):
            return "E3_Lambda_Fault_100eps"
        if run_id.startswith("E4_lambda_tuned"):
            return "E4_Lambda_100eps_Tuned"
        if run_id.startswith("E5_C1"):
            return "E5_C1_2c2g_1000eps"
        if run_id.startswith("E5_C2"):
            return "E5_C2_4c4g_1000eps"
        return run_id

    df = runs_df.copy()
    df["condition"] = df["run_id"].map(map_group)
    num_cols = [
        "emitted_events", "input_rate_eps", "steady_throughput_eps", "overall_drain_throughput_eps",
        "freshness_p50_ms", "freshness_p95_ms", "freshness_max_ms", "avg_backlog_steady", "max_backlog",
        "avg_app_cpu_pct", "peak_app_cpu_pct", "avg_app_ram_mb", "peak_app_ram_mb", "q1_p95_ms", "q2_p95_ms",
    ]
    agg = df.groupby("condition", sort=False)[num_cols].mean().round(2).reset_index()
    agg["runs"] = df.groupby("condition", sort=False)["run_id"].count().values
    agg["all_correct"] = df.groupby("condition", sort=False)["correct"].all().values
    return agg


def plot_chart1(agg: pd.DataFrame, charts_dir: Path) -> None:
    def val(cond: str, col: str) -> float:
        return float(agg.loc[agg["condition"] == cond, col].iloc[0])

    loads = ["E1: Low Load\n(20 events/s)", "E2: High Load\n(100 events/s)"]
    batch_p95_s = [val("E1_Batch_20eps", "freshness_p95_ms") / 1000.0, val("E2_Batch_100eps", "freshness_p95_ms") / 1000.0]
    lambda_p95_s = [val("E1_Lambda_20eps", "freshness_p95_ms") / 1000.0, val("E2_Lambda_100eps_Baseline", "freshness_p95_ms") / 1000.0]

    x = np.arange(len(loads))
    w = 0.34
    fig, ax = plt.subplots(figsize=(7.6, 4.4), dpi=220)
    b1 = ax.bar(x - w / 2, batch_p95_s, width=w, color="#4c78a8", edgecolor="#222", label="Option A: Batch-only (30s Spark Batch)")
    b2 = ax.bar(x + w / 2, lambda_p95_s, width=w, color="#e45756", edgecolor="#222", label="Option B: Lambda (Batch + Spark Streaming)")

    for bar in b1:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 0.6, f"{h:.2f} s\n({h*1000:,.0f} ms)", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    for bar in b2:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 0.6, f"{h:.2f} s\n({h*1000:,.0f} ms)", ha="center", va="bottom", fontsize=8.5, fontweight="bold", color="#901a19")

    ax.set_xticks(x)
    ax.set_xticklabels(loads)
    ax.set_ylabel("End-to-End Freshness p95 (seconds)")
    ax.set_ylim(0, max(batch_p95_s) * 1.33)
    ax.set_title("Chart 1: End-to-End p95 Freshness by Load Level (Batch-only vs. Lambda)")
    ax.legend(loc="upper center", ncol=2, frameon=True)
    plt.tight_layout()
    plt.savefig(charts_dir / "chart1_freshness_p95.png", dpi=220)
    plt.close(fig)


def plot_chart2(raw_dir: Path, charts_dir: Path) -> None:
    ts = pd.read_csv(raw_dir / "E3_lambda_fault_r1_timeseries.csv")
    ts = ts[(ts["rel_s"] >= 0.0) & (ts["rel_s"] <= 128.0)].copy()

    # Compute smooth 2-second bucket throughput from visible_count differences
    ts["bucket"] = (ts["rel_s"] // 2.0) * 2.0
    b_df = ts.groupby("bucket", as_index=False).agg(
        ts_epoch=("ts_epoch", "last"),
        visible_count=("visible_count", "last"),
        backlog=("backlog", "mean"),
    )
    b_df["dt"] = b_df["ts_epoch"].diff()
    b_df["dv"] = b_df["visible_count"].diff()
    b_df["thr_eps"] = (b_df["dv"] / b_df["dt"]).fillna(0.0).clip(lower=0.0)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8.4, 5.2), dpi=220, sharex=True)

    # Top: Throughput
    ax1.axvspan(40.0, 70.6, color="#f8d7da", alpha=0.55, label="Speed Layer Stopped (30s Outage)")
    ax1.axhline(100.0, color="#555", linestyle=":", linewidth=1.2, label="Target Input Rate (100 events/s)")
    ax1.plot(b_df["bucket"], b_df["thr_eps"], color="#1f77b4", linewidth=1.8, label="Serving Throughput (2s window)")
    ax1.axvline(70.6, color="#b03a2e", linestyle="--", linewidth=1.3, label="Checkpoint Restart (+70.6s)")
    ax1.set_ylabel("Throughput (events/s)")
    ax1.set_ylim(-60, 2150)
    ax1.set_title("Chart 2: E3 Fault Injection & Checkpoint Recovery — Throughput and Backlog Over Time")
    ax1.legend(loc="upper left", ncol=2, fontsize=8, frameon=True)

    # Bottom: Backlog
    ax2.axvspan(40.0, 70.6, color="#f8d7da", alpha=0.55)
    ax2.axvline(70.6, color="#b03a2e", linestyle="--", linewidth=1.3)
    ax2.fill_between(ts["rel_s"], ts["backlog"], color="#f39c12", alpha=0.35)
    ax2.plot(ts["rel_s"], ts["backlog"], color="#d35400", linewidth=1.7, label="Serving Backlog (emitted - visible)")
    peak_idx = ts["backlog"].idxmax()
    ax2.annotate(
        f"Peak backlog: {int(ts.loc[peak_idx, 'backlog']):,} events\n@ t={ts.loc[peak_idx, 'rel_s']:.1f}s",
        xy=(ts.loc[peak_idx, "rel_s"], ts.loc[peak_idx, "backlog"]),
        xytext=(ts.loc[peak_idx, "rel_s"] - 40, 2950),
        arrowprops=dict(arrowstyle="->", color="#222", lw=1.2),
        fontsize=8.2, fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="#d35400"),
    )
    ax2.set_xlabel("Elapsed Experiment Time (seconds)")
    ax2.set_ylabel("Backlog (events)")
    ax2.set_ylim(-120, 3850)
    ax2.legend(loc="upper left", frameon=True)

    plt.tight_layout()
    plt.savefig(charts_dir / "chart2_throughput_backlog_e3.png", dpi=220)
    plt.close(fig)


def plot_chart3(agg: pd.DataFrame, raw_dir: Path, charts_dir: Path) -> None:
    def val(cond: str, col: str) -> float:
        return float(agg.loc[agg["condition"] == cond, col].iloc[0])

    # Load mean spark_ms from speed_metrics for before/after
    e2_sm = pd.concat([pd.read_csv(raw_dir / f"E2_lambda_r{r}_speed_metrics.csv") for r in (1, 2)])
    e4_sm = pd.concat([pd.read_csv(raw_dir / f"E4_lambda_tuned_r{r}_speed_metrics.csv") for r in (1, 2)])

    labels = ["E2 Baseline\n(trigger=2000ms,\nshuffle=200)", "E4 Tuned\n(trigger=500ms,\nshuffle=4)"]
    p50_vals = [val("E2_Lambda_100eps_Baseline", "freshness_p50_ms"), val("E4_Lambda_100eps_Tuned", "freshness_p50_ms")]
    p95_vals = [val("E2_Lambda_100eps_Baseline", "freshness_p95_ms"), val("E4_Lambda_100eps_Tuned", "freshness_p95_ms")]
    spark_vals = [float(e2_sm["spark_ms"].mean()), float(e4_sm["spark_ms"].mean())]
    cpu_vals = [val("E2_Lambda_100eps_Baseline", "avg_app_cpu_pct"), val("E4_Lambda_100eps_Tuned", "avg_app_cpu_pct")]
    ram_vals = [val("E2_Lambda_100eps_Baseline", "avg_app_ram_mb"), val("E4_Lambda_100eps_Tuned", "avg_app_ram_mb")]

    fig, axes = plt.subplots(1, 3, figsize=(10.5, 4.0), dpi=220)
    x = np.arange(2)
    w = 0.34

    # Panel 1: Freshness p50 / p95
    b1 = axes[0].bar(x - w / 2, p50_vals, width=w, color="#72b7b2", edgecolor="#222", label="p50 (ms)")
    b2 = axes[0].bar(x + w / 2, p95_vals, width=w, color="#e45756", edgecolor="#222", label="p95 (ms)")
    for b in list(b1) + list(b2):
        h = b.get_height()
        axes[0].text(b.get_x() + b.get_width() / 2, h + 40, f"{h:,.0f}", ha="center", va="bottom", fontsize=8)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(labels, fontsize=8.3)
    axes[0].set_ylabel("Freshness (ms)")
    axes[0].set_ylim(0, max(p95_vals) * 1.22)
    axes[0].set_title("End-to-End Freshness")
    axes[0].legend(loc="upper right", fontsize=8)

    # Panel 2: Spark Micro-Batch Processing Time
    b3 = axes[1].bar(x, spark_vals, width=0.45, color=["#4c78a8", "#54a24b"], edgecolor="#222")
    for b in b3:
        h = b.get_height()
        axes[1].text(b.get_x() + b.get_width() / 2, h + 2.5, f"{h:.1f} ms", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, fontsize=8.3)
    axes[1].set_ylabel("Mean Spark Collect Time (ms)")
    axes[1].set_ylim(0, max(spark_vals) * 1.25)
    axes[1].set_title("Micro-Batch Shuffle/Collect Cost")

    # Panel 3: Resource Cost (CPU % and RAM MB)
    ax3_r = axes[2].twinx()
    b4 = axes[2].bar(x - w / 2, cpu_vals, width=w, color="#f58518", edgecolor="#222", label="Avg App CPU (%)")
    b5 = ax3_r.bar(x + w / 2, ram_vals, width=w, color="#b279a2", edgecolor="#222", label="Avg App RAM (MB)")
    for b in b4:
        h = b.get_height()
        axes[2].text(b.get_x() + b.get_width() / 2, h + 1.5, f"{h:.1f}%", ha="center", va="bottom", fontsize=8)
    for b in b5:
        h = b.get_height()
        ax3_r.text(b.get_x() + b.get_width() / 2, h + 40, f"{h:,.0f}", ha="center", va="bottom", fontsize=8)
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(labels, fontsize=8.3)
    axes[2].set_ylabel("Avg App CPU (%)", color="#b35900")
    ax3_r.set_ylabel("Avg App RAM (MB)", color="#6d3e60")
    axes[2].set_ylim(0, max(cpu_vals) * 1.32)
    ax3_r.set_ylim(0, max(ram_vals) * 1.32)
    axes[2].set_title("Resource Trade-off (CPU & RAM)")
    h1, l1 = axes[2].get_legend_handles_labels()
    h2, l2 = ax3_r.get_legend_handles_labels()
    axes[2].legend(h1 + h2, l1 + l2, loc="upper left", fontsize=7.5)

    fig.suptitle("Chart 3: E2 vs. E4 Optimization — Performance Gain vs. Resource Cost (100 events/s)", fontsize=11, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(charts_dir / "chart3_before_after_e4.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_chart4(agg: pd.DataFrame, charts_dir: Path) -> None:
    def val(cond: str, col: str) -> float:
        return float(agg.loc[agg["condition"] == cond, col].iloc[0])

    c1, c2 = "E5_C1_2c2g_1000eps", "E5_C2_4c4g_1000eps"
    labels = ["C1: 2 Cores / 2 GB", "C2: 4 Cores / 4 GB"]
    thr = [val(c1, "steady_throughput_eps"), val(c2, "steady_throughput_eps")]
    p95 = [val(c1, "freshness_p95_ms"), val(c2, "freshness_p95_ms")]
    cpu = [val(c1, "avg_app_cpu_pct"), val(c2, "avg_app_cpu_pct")]
    ram = [val(c1, "avg_app_ram_mb"), val(c2, "avg_app_ram_mb")]
    bl = [val(c1, "avg_backlog_steady"), val(c2, "avg_backlog_steady")]

    speedup = thr[1] / max(1e-6, thr[0])
    eff = (speedup / 2.0) * 100.0

    fig, axes = plt.subplots(1, 3, figsize=(10.5, 4.0), dpi=220)
    x = np.arange(2)
    w = 0.34

    # Panel 1: Throughput vs Input Rate
    b1 = axes[0].bar(x, thr, width=0.45, color=["#4c78a8", "#54a24b"], edgecolor="#222")
    axes[0].axhline(1000.0, color="#444", linestyle=":", linewidth=1.2, label="Input Rate Ceiling (1,000 eps)")
    for b in b1:
        h = b.get_height()
        axes[0].text(b.get_x() + b.get_width() / 2, h + 15, f"{h:.1f} eps", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(labels, fontsize=8.5)
    axes[0].set_ylabel("Steady Throughput (events/s)")
    axes[0].set_ylim(0, 1360)
    axes[0].set_title("Sustained Throughput (Input-Bounded)")
    axes[0].legend(loc="lower center", fontsize=7.8)
    axes[0].text(
        0.5, 0.88,
        f"Observed S = {speedup:.3f}, E = {eff:.1f}%\n(Input / 1-partition bounded)",
        transform=axes[0].transAxes, ha="center", va="center", fontsize=7.6,
        bbox=dict(boxstyle="round,pad=0.25", facecolor="#fff9e6", edgecolor="#b7950b"),
    )

    # Panel 2: p95 Freshness & Avg Backlog
    ax2_r = axes[1].twinx()
    b2 = axes[1].bar(x - w / 2, p95, width=w, color="#e45756", edgecolor="#222", label="p95 Freshness (ms)")
    b3 = ax2_r.bar(x + w / 2, bl, width=w, color="#f39c12", edgecolor="#222", label="Avg Backlog (events)")
    for b in b2:
        h = b.get_height()
        axes[1].text(b.get_x() + b.get_width() / 2, h + 18, f"{h:,.0f}", ha="center", va="bottom", fontsize=8)
    for b in b3:
        h = b.get_height()
        ax2_r.text(b.get_x() + b.get_width() / 2, h + 12, f"{h:,.0f}", ha="center", va="bottom", fontsize=8)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, fontsize=8.5)
    axes[1].set_ylabel("p95 Freshness (ms)", color="#a93226")
    ax2_r.set_ylabel("Avg Backlog (events)", color="#9c640c")
    axes[1].set_ylim(0, max(p95) * 1.28)
    ax2_r.set_ylim(0, max(bl) * 1.28)
    axes[1].set_title("Latency & Backlog Comparison")
    h1, l1 = axes[1].get_legend_handles_labels()
    h2, l2 = ax2_r.get_legend_handles_labels()
    axes[1].legend(h1 + h2, l1 + l2, loc="upper right", fontsize=7.5)

    # Panel 3: CPU & RAM utilization
    ax3_r = axes[2].twinx()
    b4 = axes[2].bar(x - w / 2, cpu, width=w, color="#f58518", edgecolor="#222", label="Avg App CPU (%)")
    b5 = ax3_r.bar(x + w / 2, ram, width=w, color="#b279a2", edgecolor="#222", label="Avg App RAM (MB)")
    for b in b4:
        h = b.get_height()
        axes[2].text(b.get_x() + b.get_width() / 2, h + 3, f"{h:.1f}%", ha="center", va="bottom", fontsize=8)
    for b in b5:
        h = b.get_height()
        ax3_r.text(b.get_x() + b.get_width() / 2, h + 50, f"{h:,.0f}", ha="center", va="bottom", fontsize=8)
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(labels, fontsize=8.5)
    axes[2].set_ylabel("Avg App CPU (%)", color="#b35900")
    ax3_r.set_ylabel("Avg App RAM (MB)", color="#6d3e60")
    axes[2].set_ylim(0, max(cpu) * 1.32)
    ax3_r.set_ylim(0, max(ram) * 1.32)
    axes[2].set_title("Resource Footprint (CPU & RAM)")
    h1, l1 = axes[2].get_legend_handles_labels()
    h2, l2 = ax3_r.get_legend_handles_labels()
    axes[2].legend(h1 + h2, l1 + l2, loc="upper left", fontsize=7.5)

    fig.suptitle("Chart 4: E5 Scale-up Evaluation (C1: 2c/2GB vs. C2: 4c/4GB at 1,000 events/s)", fontsize=11, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(charts_dir / "chart4_scaleup_e5.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    raw_dir = METRICS_DIR / "raw"
    charts_dir = METRICS_DIR / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    runs_df = pd.read_csv(METRICS_DIR / "summary_runs.csv")
    agg = build_aggregated_summary(runs_df)
    agg.to_csv(METRICS_DIR / "summary_aggregated.csv", index=False)
    print(agg.to_string(index=False))

    plot_chart1(agg, charts_dir)
    plot_chart2(raw_dir, charts_dir)
    plot_chart3(agg, raw_dir, charts_dir)
    plot_chart4(agg, charts_dir)
    print(f"Generated 4 charts in {charts_dir}")


if __name__ == "__main__":
    main()
