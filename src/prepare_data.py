import hashlib
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from config import DATA_DIR, RAW_DATA_DIR

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
RAW_FILENAME = "yellow_tripdata_2024-01.parquet"
NORMALIZED_PARQUET = DATA_DIR / "normalized_events.parquet"
SAMPLE_JSONL = DATA_DIR / "sample_events.jsonl"
MANIFEST_PATH = DATA_DIR / "data_manifest.json"

SAMPLE_SIZE_NORMALIZED = 50_000
SAMPLE_SIZE_REPO = 1_500
SEED = 42


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_source(dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 1_000_000:
        print(f"Source file already exists: {dest}")
        return
    print(f"Downloading {SOURCE_URL} -> {dest} ...")
    urllib.request.urlretrieve(SOURCE_URL, dest)
    print(f"Downloaded {dest.stat().st_size:,} bytes")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RAW_DATA_DIR / RAW_FILENAME
    download_source(raw_path)

    file_sha256 = sha256_file(raw_path)
    table = pq.read_table(
        raw_path,
        columns=["tpep_pickup_datetime", "PULocationID", "total_amount"],
    )
    df = table.to_pandas()
    input_rows = len(df)

    # Track sequential filtering counts for manifest auditability
    mask_non_null = (
        df["tpep_pickup_datetime"].notna()
        & df["PULocationID"].notna()
        & df["total_amount"].notna()
    )
    rejected_null = int((~mask_non_null).sum())
    df = df[mask_non_null].copy()
    df["orig_idx"] = df.index.astype(np.int64)

    df["PULocationID"] = df["PULocationID"].astype(np.int64)
    mask_zone = (df["PULocationID"] >= 1) & (df["PULocationID"] <= 265)
    rejected_zone = int((~mask_zone).sum())
    df = df[mask_zone].copy()

    mask_amount = (df["total_amount"] > 0.0) & (df["total_amount"] <= 500.0)
    rejected_amount = int((~mask_amount).sum())
    df = df[mask_amount].copy()

    pickup_dt = pd.to_datetime(df["tpep_pickup_datetime"], utc=True, errors="coerce")
    mask_date = (
        pickup_dt.notna()
        & (pickup_dt >= pd.Timestamp("2024-01-01T00:00:00Z"))
        & (pickup_dt < pd.Timestamp("2024-02-01T00:00:00Z"))
    )
    rejected_date = int((~mask_date).sum())
    df = df[mask_date].copy()
    df["pickup_utc"] = pickup_dt[mask_date]

    valid_rows = len(df)
    rejected_rows = input_rows - valid_rows

    # Deterministic sampling preserves reproducible category and amount distributions
    sampled = df.sample(n=SAMPLE_SIZE_NORMALIZED, random_state=SEED).reset_index(drop=True)
    sampled["amount_cents"] = (sampled["total_amount"] * 100.0).round().astype(np.int64)
    sampled = sampled[sampled["amount_cents"] > 0].copy()

    sampled["event_time"] = sampled["pickup_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    sampled["category"] = sampled["PULocationID"].map(lambda z: f"zone_{int(z):03d}")
    sampled["event_id"] = [
        hashlib.sha256(
            f"tlc202401:{oid}:{et}:{cat}:{amt}".encode("utf-8")
        ).hexdigest()[:24]
        for oid, et, cat, amt in zip(
            sampled["orig_idx"],
            sampled["event_time"],
            sampled["category"],
            sampled["amount_cents"],
        )
    ]

    out_df = sampled[["event_id", "event_time", "category", "amount_cents"]].copy()
    out_df.to_parquet(NORMALIZED_PARQUET, index=False)

    # Save >= 1,000 sample events to git-tracked JSONL
    repo_sample = out_df.head(SAMPLE_SIZE_REPO)
    with SAMPLE_JSONL.open("w", encoding="utf-8") as f:
        for rec in repo_sample.to_dict(orient="records"):
            f.write(json.dumps(rec) + "\n")

    manifest = {
        "source_name": "NYC TLC Yellow Taxi Trip Records (January 2024)",
        "source_page": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        "download_url": SOURCE_URL,
        "download_date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "filename": RAW_FILENAME,
        "sha256": file_sha256,
        "input_rows": input_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "rejection_breakdown": {
            "null_required_fields": rejected_null,
            "invalid_pu_location_id": rejected_zone,
            "out_of_range_total_amount": rejected_amount,
            "out_of_month_pickup_datetime": rejected_date,
        },
        "cleaning_rules": [
            "Drop rows where tpep_pickup_datetime, PULocationID, or total_amount is NULL",
            "Keep valid TLC zones: 1 <= PULocationID <= 265",
            "Keep positive non-outlier fares: 0 < total_amount <= 500.0 USD; convert to integer amount_cents = round(total_amount * 100)",
            "Keep business timestamps within January 2024: 2024-01-01T00:00:00Z <= tpep_pickup_datetime < 2024-02-01T00:00:00Z",
        ],
        "sampling": {
            "seed": SEED,
            "normalized_pool_size": len(out_df),
            "repo_sample_size": len(repo_sample),
            "distinct_categories": int(out_df["category"].nunique()),
        },
        "schema_notes": {
            "event_id": "24-char SHA-256 hex digest of source row index and canonical fields",
            "event_time": "Original tpep_pickup_datetime formatted as ISO-8601 UTC",
            "category": "Pickup taxi zone (zone_001 .. zone_265)",
            "amount_cents": "Integer total_amount * 100",
        },
    }

    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
