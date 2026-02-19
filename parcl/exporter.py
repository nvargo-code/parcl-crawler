"""Export parcel_features view to CSV, Parquet, or JSONL."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from parcl.config import PROJECT_ROOT
from parcl.db import Database
from parcl.logger import get_logger

log = get_logger("exporter")

# Columns in the parcel_features view
VIEW_COLUMNS = [
    "parcel_id", "address", "address_norm", "city", "state", "zip_code",
    "county", "latitude", "longitude", "base_zoning", "zoning_desc",
    "lot_size_sqft", "apn", "total_permits", "permits_5yr", "active_permits",
    "total_zoning_cases", "open_zoning_cases", "total_boa_cases",
    "environmental_flags", "overlay_count", "fetched_at",
]


def export_data(db: Database, fmt: str, output_dir: str | None = None) -> str:
    """Export parcel_features view to the specified format.

    Returns the output file path.
    """
    out_dir = Path(output_dir) if output_dir else PROJECT_ROOT / "data" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = db.fetchall("SELECT * FROM parcel_features")

    if fmt == "csv":
        return _export_csv(rows, out_dir)
    elif fmt == "parquet":
        return _export_parquet(rows, out_dir)
    elif fmt == "jsonl":
        return _export_jsonl(rows, out_dir)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def _export_csv(rows: list[tuple], out_dir: Path) -> str:
    path = out_dir / "parcel_features.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(VIEW_COLUMNS)
        writer.writerows(rows)
    log.info(f"Exported {len(rows)} rows to {path}")
    return str(path)


def _export_parquet(rows: list[tuple], out_dir: Path) -> str:
    import pandas as pd

    path = out_dir / "parcel_features.parquet"
    df = pd.DataFrame(rows, columns=VIEW_COLUMNS)
    df.to_parquet(str(path), index=False)
    log.info(f"Exported {len(rows)} rows to {path}")
    return str(path)


def _export_jsonl(rows: list[tuple], out_dir: Path) -> str:
    path = out_dir / "parcel_features.jsonl"
    with open(path, "w") as f:
        for row in rows:
            record = dict(zip(VIEW_COLUMNS, row))
            f.write(json.dumps(record, default=str) + "\n")
    log.info(f"Exported {len(rows)} rows to {path}")
    return str(path)
