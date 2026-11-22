from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


def draw_box(ax, x, y, w, h, title, subtitle="", facecolor="#f4f6f9", edgecolor="#2c3e50", lw=1.4):
    box = mpatches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.015,rounding_size=0.02",
        facecolor=facecolor, edgecolor=edgecolor, linewidth=lw,
    )
    ax.add_patch(box)
    if subtitle:
        ax.text(x + w / 2, y + h * 0.64, title, ha="center", va="center", fontsize=9.5, fontweight="bold", color="#111")
        ax.text(x + w / 2, y + h * 0.30, subtitle, ha="center", va="center", fontsize=7.6, color="#333")
    else:
        ax.text(x + w / 2, y + h / 2, title, ha="center", va="center", fontsize=9.5, fontweight="bold", color="#111")


def draw_arrow(ax, x1, y1, x2, y2, label="", rad=0.0, color="#2c3e50", ls="-", lx_off=0.0, ly_off=0.02):
    ax.annotate(
        "",
        xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle="-|>,head_width=0.25,head_length=0.45", color=color, lw=1.4, linestyle=ls, connectionstyle=f"arc3,rad={rad}"),
    )
    if label:
        mx, my = (x1 + x2) / 2 + lx_off, (y1 + y2) / 2 + ly_off
        ax.text(mx, my, label, ha="center", va="center", fontsize=7.5, color=color,
                bbox=dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="none", alpha=0.85))


def main() -> None:
    out_path = Path(__file__).resolve().parent.parent / "report" / "architecture_diagram.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(11.8, 4.9), dpi=220)
    ax.set_xlim(-0.01, 1.03)
    ax.set_ylim(0.0, 1.02)
    ax.axis("off")

    # Ingestion & source
    draw_box(ax, 0.02, 0.38, 0.16, 0.24, "NYC TLC Replay", "producer.py\nseq=1..N, UTC produced_at\n(50k valid events, seed=42)", facecolor="#eaf2f8", edgecolor="#1f618d")
    draw_box(ax, 0.23, 0.38, 0.15, 0.24, "Apache Kafka", "KRaft Broker\nTopic: nyc_taxi_events\n(1 Partition)", facecolor="#fef9e7", edgecolor="#b7950b")

    # Batch Branch (Top)
    draw_box(ax, 0.43, 0.68, 0.19, 0.22, "Raw Storage", "raw_consumer.py\nImmutable Snappy Parquet\n+ raw_manifest.json (H)", facecolor="#eefaf0", edgecolor="#1e8449")
    draw_box(ax, 0.67, 0.68, 0.16, 0.22, "Batch Layer (A & B)", "Spark Batch (every 30s)\nRead seq <= H\nCOUNT/SUM per category", facecolor="#eefaf0", edgecolor="#1e8449")

    # Speed Branch (Bottom - B only)
    draw_box(ax, 0.47, 0.10, 0.32, 0.22, "Speed Layer (Option B - Lambda Only)", "Spark Structured Streaming + Checkpoint\nIdempotent Upsert: ON CONFLICT (event_id) DO NOTHING\nWrites speed_events (seq > H)", facecolor="#fdedec", edgecolor="#b03a2e")

    # Serving & Queries (Right)
    draw_box(ax, 0.87, 0.33, 0.12, 0.34, "PostgreSQL 16\nServing Layer", "pipeline_state (H)\nbatch_category_agg\nspeed_events\nView:\nBatch(<=H)+Speed(>H)", facecolor="#f4ecf7", edgecolor="#6c3483")

    # Arrows
    draw_arrow(ax, 0.18, 0.50, 0.23, 0.50, "acks=1", ly_off=0.04)
    draw_arrow(ax, 0.38, 0.55, 0.43, 0.78, "consume", rad=-0.12, ly_off=0.05)
    draw_arrow(ax, 0.62, 0.79, 0.67, 0.79, "snapshot\n<= H", ly_off=0.06)
    draw_arrow(ax, 0.83, 0.76, 0.87, 0.56, "Atomic TX:\nupdate agg,\nH & prune", rad=-0.1, lx_off=0.01, ly_off=0.09, color="#1e8449")

    draw_arrow(ax, 0.38, 0.45, 0.47, 0.21, "stream\n(Lambda B)", rad=0.12, lx_off=-0.03, ly_off=-0.05, color="#b03a2e")
    draw_arrow(ax, 0.79, 0.21, 0.87, 0.42, "idempotent\nupsert", rad=0.12, lx_off=0.02, ly_off=-0.06, color="#b03a2e")

    # Query & Monitor annotations
    ax.text(0.93, 0.12, "Queries Q1 & Q2\n+ 200ms Polling Observer\n(Freshness, Backlog, Accuracy)", ha="center", va="center", fontsize=7.8,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#fbfcfc", edgecolor="#6c3483", linestyle="--"))
    draw_arrow(ax, 0.93, 0.21, 0.93, 0.33, "", color="#6c3483", ls="--")

    ax.set_title("Lambda Architecture Deployment (Option A: Batch-only vs. Option B: Batch + Speed)", fontsize=11.5, fontweight="bold", pad=8)
    plt.tight_layout()
    plt.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


if __name__ == "__main__":
    main()
