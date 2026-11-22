import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from config import BASE_DIR, LOGS_DIR, METRICS_DIR, RUNTIME_DIR, SPARK_PACKAGES


def reset_state() -> None:
    """Reset Kafka topic, PostgreSQL serving tables, and runtime directories."""
    subprocess.run(
        [
            "docker", "exec", "hw4-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--delete", "--topic", "nyc_taxi_events",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)
    for _ in range(10):
        res = subprocess.run(
            [
                "docker", "exec", "hw4-kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--create", "--if-not-exists",
                "--topic", "nyc_taxi_events",
                "--partitions", "1",
                "--replication-factor", "1",
            ],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0:
            break
        time.sleep(1.0)

    subprocess.run(
        [
            "docker", "exec", "hw4-postgres",
            "psql", "-U", "lambda", "-d", "lambda_serving", "-c",
            "TRUNCATE TABLE batch_category_agg, speed_events; "
            "UPDATE pipeline_state SET batch_watermark_seq = 0, batch_runs = 0, updated_at = NOW() WHERE id = 1;",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )

    if RUNTIME_DIR.exists():
        shutil.rmtree(RUNTIME_DIR)
    (RUNTIME_DIR / "raw_parquet").mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "spark_checkpoint").mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def wait_for_log_token(log_path: Path, token: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if log_path.exists():
            txt = log_path.read_text(encoding="utf-8", errors="ignore")
            if token in txt:
                return
        time.sleep(0.3)
    raise TimeoutError(f"Timed out waiting for '{token}' in {log_path}")


def stop_proc(proc: subprocess.Popen | None, name: str) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        print(f"[runner] killing {name} after timeout")
        proc.kill()
        proc.wait()


def run_single_experiment(
    run_id: str,
    mode: str,
    phases: str,
    cores: int = 2,
    memory: str = "2g",
    batch_interval: float = 30.0,
    trigger_ms: int = 2000,
    shuffle_partitions: int = 200,
    max_offsets: int = 0,
    write_mode: str = "batch",
    warmup_sec: float = 20.0,
    fault_stop_at_s: float | None = None,
    fault_down_s: float = 30.0,
    drain_timeout: float = 55.0,
) -> dict:
    print(f"\n=== Starting {run_id} (mode={mode}, phases={phases}, cores={cores}, mem={memory}, trigger={trigger_ms}ms, shuffle={shuffle_partitions}, write={write_mode}) ===")
    reset_state()
    raw_out = METRICS_DIR / "raw"
    raw_out.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR / "src")
    env["PYTHONUNBUFFERED"] = "1"

    raw_log = LOGS_DIR / f"{run_id}_raw.log"
    batch_log = LOGS_DIR / f"{run_id}_batch.log"
    speed_log = LOGS_DIR / f"{run_id}_speed.log"
    mon_log = LOGS_DIR / f"{run_id}_monitor.log"
    prod_log = LOGS_DIR / f"{run_id}_producer.log"

    raw_proc = batch_proc = speed_proc = mon_proc = None
    try:
        raw_proc = subprocess.Popen(
            [sys.executable, "src/raw_consumer.py"],
            cwd=BASE_DIR, env=env, stdout=raw_log.open("w"), stderr=subprocess.STDOUT,
        )
        wait_for_log_token(raw_log, "[raw_consumer] started", timeout_s=20)

        batch_proc = subprocess.Popen(
            [
                sys.executable, "src/batch_layer.py",
                "--cores", str(cores),
                "--memory", memory,
                "--interval", str(batch_interval),
                "--shuffle-partitions", str(min(shuffle_partitions, 16)),
            ],
            cwd=BASE_DIR, env=env, stdout=batch_log.open("w"), stderr=subprocess.STDOUT,
        )
        wait_for_log_token(batch_log, "[batch_layer] started", timeout_s=60)

        speed_cmd = [
            sys.executable, "src/speed_layer.py",
            "--cores", str(cores),
            "--memory", memory,
            "--trigger-ms", str(trigger_ms),
            "--shuffle-partitions", str(shuffle_partitions),
            "--max-offsets", str(max_offsets),
            "--write-mode", write_mode,
        ]
        if mode == "lambda":
            speed_proc = subprocess.Popen(
                speed_cmd,
                cwd=BASE_DIR, env=env, stdout=speed_log.open("w"), stderr=subprocess.STDOUT,
            )
            wait_for_log_token(speed_log, "[speed_layer] started", timeout_s=90)

        pids = [p.pid for p in [raw_proc, batch_proc, speed_proc] if p is not None]
        mon_proc = subprocess.Popen(
            [
                sys.executable, "src/monitor.py",
                "--run-id", run_id,
                "--poll-ms", "200",
                "--drain-timeout", str(drain_timeout),
                "--pids", ",".join(map(str, pids)),
                "--out-dir", str(raw_out),
            ],
            cwd=BASE_DIR, env=env, stdout=mon_log.open("w"), stderr=subprocess.STDOUT,
        )

        prod_proc = subprocess.Popen(
            [
                sys.executable, "src/producer.py",
                "--phases", phases,
                "--warmup-sec", str(warmup_sec),
            ],
            cwd=BASE_DIR, env=env, stdout=prod_log.open("w"), stderr=subprocess.STDOUT,
        )

        t_start = time.monotonic()
        if fault_stop_at_s is not None and mode == "lambda":
            while prod_proc.poll() is None and (time.monotonic() - t_start) < fault_stop_at_s:
                time.sleep(0.2)
            print(f"[runner] E3 fault injection: stopping speed_layer at +{time.monotonic() - t_start:.1f}s for {fault_down_s}s")
            stop_proc(speed_proc, "speed_layer")
            time.sleep(fault_down_s)
            print(f"[runner] E3 recovery: restarting speed_layer from checkpoint at +{time.monotonic() - t_start:.1f}s")
            speed_proc = subprocess.Popen(
                speed_cmd,
                cwd=BASE_DIR, env=env, stdout=speed_log.open("a"), stderr=subprocess.STDOUT,
            )
            wait_for_log_token(speed_log, "[speed_layer] started", timeout_s=90)

        prod_proc.wait()
        # Allow raw_consumer to flush tail events, then wait for monitor drain
        time.sleep(2.0)
        mon_proc.wait(timeout=drain_timeout + 30)

    finally:
        stop_proc(speed_proc, "speed_layer")
        stop_proc(batch_proc, "batch_layer")
        stop_proc(raw_proc, "raw_consumer")

    # Preserve component metrics for bottleneck analysis
    for fname in ["batch_metrics.csv", "speed_metrics.csv", "producer_summary.json"]:
        src = RUNTIME_DIR / fname
        if src.exists():
            shutil.copy2(src, raw_out / f"{run_id}_{fname}")

    summary_path = raw_out / f"{run_id}_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    print(f"=== Finished {run_id}: p95={summary['freshness_p95_ms']}ms, thr={summary['steady_throughput_eps']} eps, correct={summary['correctness']['correct']} ===")
    return summary


def prewarm() -> None:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("HW4-Prewarm")
        .master("local[2]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    print(f"Prewarmed Spark {spark.version} with {SPARK_PACKAGES}")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment orchestrator for HW4")
    parser.add_argument("--prewarm", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--suite", choices=["e1", "e2", "e3", "e4", "e5", "baseline", "all"], default=None)
    parser.add_argument("--probe", default="", help="Run quick probe e.g. '1000:20:2:2g'")
    args = parser.parse_args()

    if args.probe:
        r_s, d_s, c_s, m_s = args.probe.split(":")
        run_single_experiment(
            run_id=f"probe_c{c_s}_{r_s}",
            mode="lambda",
            phases=f"{r_s}:{d_s}",
            cores=int(c_s),
            memory=m_s,
            batch_interval=30.0,
            trigger_ms=500,
            shuffle_partitions=4,
            write_mode="batch",
            warmup_sec=3.0,
            drain_timeout=30.0,
        )
        return

    if args.prewarm:
        prewarm()
        return
    if args.reset:
        reset_state()
        print("State reset complete.")
        return
    if args.smoke:
        run_single_experiment(
            run_id="smoke_lambda",
            mode="lambda",
            phases="50:20",
            cores=2,
            memory="2g",
            batch_interval=15.0,
            trigger_ms=2000,
            shuffle_partitions=200,
            write_mode="row",
            warmup_sec=5.0,
            drain_timeout=25.0,
        )
        return

    if args.suite in ("e1", "baseline", "all"):
        for r in (1, 2):
            run_single_experiment(f"E1_batch_r{r}", mode="batch", phases="20:120", cores=2, memory="2g", batch_interval=30.0)
        for r in (1, 2):
            run_single_experiment(f"E1_lambda_r{r}", mode="lambda", phases="20:120", cores=2, memory="2g", batch_interval=30.0, trigger_ms=2000, shuffle_partitions=200, write_mode="batch")

    if args.suite in ("e2", "baseline", "all"):
        for r in (1, 2):
            run_single_experiment(f"E2_batch_r{r}", mode="batch", phases="100:120", cores=2, memory="2g", batch_interval=30.0)
        for r in (1, 2):
            run_single_experiment(f"E2_lambda_r{r}", mode="lambda", phases="100:120", cores=2, memory="2g", batch_interval=30.0, trigger_ms=2000, shuffle_partitions=200, write_mode="batch")

    if args.suite in ("e3", "baseline", "all"):
        run_single_experiment(
            "E3_lambda_fault_r1",
            mode="lambda",
            phases="100:120",
            cores=2,
            memory="2g",
            batch_interval=30.0,
            trigger_ms=2000,
            shuffle_partitions=200,
            write_mode="batch",
            fault_stop_at_s=40.0,
            fault_down_s=30.0,
        )

    if args.suite in ("e4", "all"):
        # E4 tuned parameters will be verified against observed E1/E2/E3 bottleneck
        for r in (1, 2):
            run_single_experiment(
                f"E4_lambda_tuned_r{r}",
                mode="lambda",
                phases="100:120",
                cores=2,
                memory="2g",
                batch_interval=30.0,
                trigger_ms=500,
                shuffle_partitions=4,
                write_mode="batch",
            )

    if args.suite in ("e5", "all"):
        # E5 scale-up C1 (2 cores / 2g) vs C2 (4 cores / 4g) at 10x E2 replay rate (1000 eps x 120s)
        for r in (1, 2):
            run_single_experiment(
                f"E5_C1_2c2g_r{r}",
                mode="lambda",
                phases="1000:120",
                cores=2,
                memory="2g",
                batch_interval=30.0,
                trigger_ms=500,
                shuffle_partitions=4,
                write_mode="batch",
            )
        for r in (1, 2):
            run_single_experiment(
                f"E5_C2_4c4g_r{r}",
                mode="lambda",
                phases="1000:120",
                cores=4,
                memory="4g",
                batch_interval=30.0,
                trigger_ms=500,
                shuffle_partitions=4,
                write_mode="batch",
            )


if __name__ == "__main__":
    main()
