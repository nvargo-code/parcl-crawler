"""Orchestrates Extract -> Transform -> Load for a single source."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from parcl.config import SourceConfig, load_settings
from parcl.db import Database
from parcl.etl.loader import load_records
from parcl.etl.transformer import transform_batch
from parcl.logger import get_logger
from parcl.sources import get_source_class

log = get_logger("pipeline")


def _ensure_source_registered(db: Database, source_config: SourceConfig) -> None:
    """Ensure the source is registered in the sources table."""
    db.execute(
        "INSERT INTO sources (id, name, source_type, base_url, dataset_id, "
        "target_table, jurisdiction_id, license, refresh_cadence) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (id) DO NOTHING",
        (
            source_config.id,
            source_config.id.replace("_", " ").title(),
            source_config.source_type,
            source_config.base_url,
            source_config.dataset_id,
            source_config.target_table,
            source_config.jurisdiction_id,
            source_config.license,
            source_config.refresh_cadence,
        ),
    )
    db.commit()


def run_source(
    source_config: SourceConfig,
    db: Database,
) -> dict[str, Any]:
    """Run the full ETL pipeline for a single source.

    Returns a summary dict with rows loaded, duration, errors, etc.
    """
    settings = load_settings()
    start = time.time()

    log.info(f"Starting ETL for source '{source_config.id}'")

    # Register source in DB
    _ensure_source_registered(db, source_config)

    # Instantiate the correct plugin
    source_cls = get_source_class(source_config.source_type)
    source = source_cls(source_config, settings.crawler)

    total_raw = 0
    total_loaded = 0
    page_count = 0
    errors = 0

    try:
        for batch in source.fetch():
            page_count += 1
            total_raw += len(batch)

            # Transform
            transformed = transform_batch(batch, source_config)
            skipped = len(batch) - len(transformed)
            if skipped > 0:
                log.debug(f"Page {page_count}: skipped {skipped} records (missing required fields)")

            # Load
            try:
                loaded = load_records(db, source_config.target_table, transformed)
                total_loaded += loaded
            except Exception as e:
                errors += 1
                log.error(f"Load error on page {page_count}: {e}")

            log.info(f"Page {page_count}: {len(transformed)} transformed, {loaded} loaded")

    except Exception as e:
        errors += 1
        log.error(f"Fetch error for source '{source_config.id}': {e}")

    duration = time.time() - start

    # Update source metadata
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE sources SET last_run_at = ?, last_row_count = ? WHERE id = ?",
        (now, total_loaded, source_config.id),
    )
    db.commit()

    summary = {
        "source_id": source_config.id,
        "target_table": source_config.target_table,
        "pages": page_count,
        "raw_records": total_raw,
        "loaded_records": total_loaded,
        "errors": errors,
        "duration_seconds": round(duration, 2),
    }
    log.info(f"ETL complete: {summary}")
    return summary
