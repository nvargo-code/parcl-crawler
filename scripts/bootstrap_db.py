#!/usr/bin/env python3
"""Quick schema init script — equivalent to `parcl init`."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parcl.config import load_settings
from parcl.db import create_database, init_schema
from parcl.logger import setup_logging

if __name__ == "__main__":
    settings = load_settings()
    setup_logging(settings.logging_level, settings.logging_format)
    db = create_database(settings.database)
    init_schema(db)
    print("Database initialized successfully.")
    counts = db.table_row_counts()
    for table, count in counts.items():
        print(f"  {table}: {count} rows")
    db.close()
