import argparse
from pathlib import Path

import pandas as pd
import psycopg2

from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER, RUNTIME_DIR


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify serving queries Q1, Q2 and ground-truth correctness")
    parser.add_argument("--emitted-csv", type=Path, default=RUNTIME_DIR / "emitted_events.csv")
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            print("=== Pipeline State ===")
            cur.execute("SELECT batch_watermark_seq, batch_runs, updated_at FROM pipeline_state WHERE id = 1;")
            wm, runs, updated_at = cur.fetchone()
            print(f"Watermark H = {wm}, batch_runs = {runs}, updated_at = {updated_at}")

            print(f"\n=== Q1: Top {args.top_k} Categories by total_amount_cents ===")
            cur.execute(
                """
                SELECT category, event_count, total_amount_cents
                FROM serving_category_agg
                ORDER BY total_amount_cents DESC, category ASC
                LIMIT %s;
                """,
                (args.top_k,),
            )
            for cat, cnt, amt in cur.fetchall():
                print(f"  {cat:10s} | count={cnt:8d} | amount_cents={amt:12d}")

            print("\n=== Q2: System Total ===")
            cur.execute(
                """
                SELECT COALESCE(SUM(event_count), 0)::BIGINT,
                       COALESCE(SUM(total_amount_cents), 0)::BIGINT
                FROM serving_category_agg;
                """
            )
            pg_cnt, pg_amt = cur.fetchone()
            print(f"  Serving total_count = {pg_cnt}, total_amount_cents = {pg_amt}")

            if args.emitted_csv.exists():
                df = pd.read_csv(args.emitted_csv).drop_duplicates(subset=["event_id"])
                gt_cnt = int(len(df))
                gt_amt = int(df["amount_cents"].sum())

                cur.execute("SELECT category, event_count, total_amount_cents FROM serving_category_agg;")
                pg_map = {r[0]: (int(r[1]), int(r[2])) for r in cur.fetchall()}
                gt_map = {
                    cat: (int(grp["amount_cents"].count()), int(grp["amount_cents"].sum()))
                    for cat, grp in df.groupby("category")
                }
                cat_diffs = [
                    c for c in (set(pg_map) | set(gt_map)) if pg_map.get(c, (0, 0)) != gt_map.get(c, (0, 0))
                ]
                print("\n=== Ground-Truth Reconciliation ===")
                print(f"  Ground truth count = {gt_cnt}, sum_cents = {gt_amt}")
                print(f"  Diff count = {int(pg_cnt) - gt_cnt}, Diff sum_cents = {int(pg_amt) - gt_amt}, Category mismatches = {len(cat_diffs)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
