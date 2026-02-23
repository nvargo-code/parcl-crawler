"""Database factory: DuckDB (default) or PostgreSQL."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from parcl.config import DatabaseConfig, PROJECT_ROOT, Settings, load_settings
from parcl.logger import get_logger

log = get_logger("db")


class Database:
    """Thin wrapper over DuckDB or PostgreSQL connection."""

    def __init__(self, conn: Any, db_type: str):
        self.conn = conn
        self.db_type = db_type

    def execute(self, sql: str, params: tuple | list | None = None) -> Any:
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)

    def executemany(self, sql: str, params_list: list[tuple]) -> None:
        if self.db_type == "duckdb":
            for params in params_list:
                self.conn.execute(sql, params)
        else:
            cur = self.conn.cursor()
            cur.executemany(sql, params_list)
            self.conn.commit()

    def fetchall(self, sql: str, params: tuple | list | None = None) -> list[tuple]:
        result = self.execute(sql, params)
        if self.db_type == "duckdb":
            return result.fetchall()
        else:
            cur = result if hasattr(result, "fetchall") else self.conn.cursor()
            return cur.fetchall()

    def fetchone(self, sql: str, params: tuple | list | None = None) -> tuple | None:
        result = self.execute(sql, params)
        if self.db_type == "duckdb":
            return result.fetchone()
        else:
            cur = result if hasattr(result, "fetchone") else self.conn.cursor()
            return cur.fetchone()

    def commit(self) -> None:
        if self.db_type == "postgresql":
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def table_row_counts(self) -> dict[str, int]:
        """Return row count for every user table."""
        tables = [
            "sources", "jurisdictions", "parcels", "permits",
            "zoning_cases", "boa_cases", "zoning_overlays",
            "utility_capacity", "environmental_constraints",
            "rights_restrictions", "property_valuations", "transit_amenities",
        ]
        counts = {}
        for t in tables:
            try:
                row = self.fetchone(f"SELECT COUNT(*) FROM {t}")
                counts[t] = row[0] if row else 0
            except Exception:
                counts[t] = -1  # table doesn't exist yet
        return counts


def create_database(config: DatabaseConfig | None = None) -> Database:
    """Create and return a Database instance."""
    if config is None:
        config = load_settings().database

    if config.type == "duckdb":
        import duckdb
        db_path = PROJECT_ROOT / config.duckdb_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_path))
        log.info(f"Connected to DuckDB at {db_path}")
        return Database(conn, "duckdb")

    elif config.type == "postgresql":
        import psycopg2
        url = config.postgres_url or os.environ.get("DATABASE_URL", "")
        conn = psycopg2.connect(url)
        log.info("Connected to PostgreSQL")
        return Database(conn, "postgresql")

    else:
        raise ValueError(f"Unsupported database type: {config.type}")


def init_schema(db: Database) -> None:
    """Create all tables and views from SQL files."""
    json_type = "JSON" if db.db_type == "duckdb" else "JSONB"

    schema_path = PROJECT_ROOT / "sql" / "schema.sql"
    views_path = PROJECT_ROOT / "sql" / "views.sql"

    schema_sql = schema_path.read_text().replace("{JSON_TYPE}", json_type)
    views_sql = views_path.read_text().replace("{JSON_TYPE}", json_type)

    # Execute each statement separately
    for sql in schema_sql.split(";"):
        sql = sql.strip()
        if sql:
            db.execute(sql)

    for sql in views_sql.split(";"):
        sql = sql.strip()
        if sql:
            db.execute(sql)

    db.commit()

    # Seed jurisdictions for Austin v1 (inserted in order for FK integrity)
    seeds = [
        ("us", "United States", "country", None, None),
        ("us-tx", "Texas", "state", "us", "TX"),
        ("us-tx-travis", "Travis County", "county", "us-tx", "TX"),
        ("austin-tx", "Austin", "city", "us-tx-travis", "TX"),
    ]
    for jid, name, level, parent, state in seeds:
        existing = db.fetchone("SELECT id FROM jurisdictions WHERE id = ?", (jid,))
        if not existing:
            db.execute(
                "INSERT INTO jurisdictions (id, name, level, parent_id, state_code) "
                "VALUES (?, ?, ?, ?, ?)",
                (jid, name, level, parent, state),
            )
    db.commit()
    log.info("Schema initialized with all tables and views")
