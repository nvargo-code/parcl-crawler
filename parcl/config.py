"""Load and validate YAML configs for parcl-crawler."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _find_project_root() -> Path:
    """Walk up from this file to find the project root (where config/ lives)."""
    d = Path(__file__).resolve().parent.parent
    if (d / "config").is_dir():
        return d
    # Fallback to CWD
    return Path.cwd()


PROJECT_ROOT = _find_project_root()


@dataclass
class DatabaseConfig:
    type: str = "duckdb"
    duckdb_path: str = "data/parcl.duckdb"
    postgres_url: str | None = None


@dataclass
class CrawlerConfig:
    rate_limit_seconds: float = 0.5
    page_size: int = 1000
    max_pages: int = 500
    timeout_seconds: int = 60
    max_retries: int = 3
    retry_backoff: float = 2.0


@dataclass
class FieldMapping:
    raw_field: str
    schema_field: str
    type: str = "text"
    required: bool = False
    template: str | None = None  # e.g. "{year}-{month}-01" to combine multiple raw fields


@dataclass
class SourceConfig:
    id: str
    source_type: str
    target_table: str
    jurisdiction_id: str = "austin-tx"
    base_url: str = ""
    dataset_id: str = ""
    license: str = ""
    refresh_cadence: str = ""
    external_id_template: str | None = None  # e.g. "{plant}_{year}_{month}"
    filters: dict[str, Any] = field(default_factory=dict)
    field_map: list[FieldMapping] = field(default_factory=list)
    layers: list[dict[str, Any]] | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceConfig:
        fm = [
            FieldMapping(**m) if isinstance(m, dict) else m
            for m in data.pop("field_map", [])
        ]
        layers = data.pop("layers", None)
        extra = {}
        known = {f.name for f in cls.__dataclass_fields__.values()}
        for k in list(data.keys()):
            if k not in known:
                extra[k] = data.pop(k)
        return cls(**data, field_map=fm, layers=layers, extra=extra)


@dataclass
class Settings:
    database: DatabaseConfig
    crawler: CrawlerConfig
    logging_level: str = "INFO"
    logging_format: str = "structured"
    sources_dir: str = "config/sources"
    export_output_dir: str = "data/exports"


def load_settings(path: Path | None = None) -> Settings:
    """Load settings.yaml from project root or given path."""
    if path is None:
        path = PROJECT_ROOT / "config" / "settings.yaml"
    with open(path) as f:
        raw = yaml.safe_load(f)

    db_raw = raw.get("database", {})
    db = DatabaseConfig(
        type=db_raw.get("type", "duckdb"),
        duckdb_path=db_raw.get("duckdb_path", "data/parcl.duckdb"),
        postgres_url=db_raw.get("postgres_url"),
    )
    cr_raw = raw.get("crawler", {})
    crawler = CrawlerConfig(
        rate_limit_seconds=cr_raw.get("rate_limit_seconds", 0.5),
        page_size=cr_raw.get("page_size", 1000),
        max_pages=cr_raw.get("max_pages", 500),
        timeout_seconds=cr_raw.get("timeout_seconds", 60),
        max_retries=cr_raw.get("max_retries", 3),
        retry_backoff=cr_raw.get("retry_backoff", 2.0),
    )
    log_raw = raw.get("logging", {})
    return Settings(
        database=db,
        crawler=crawler,
        logging_level=log_raw.get("level", "INFO"),
        logging_format=log_raw.get("format", "structured"),
        sources_dir=raw.get("sources_dir", "config/sources"),
        export_output_dir=raw.get("export", {}).get("output_dir", "data/exports"),
    )


def load_source_config(path: Path) -> SourceConfig:
    """Load a single source YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return SourceConfig.from_dict(raw)


def load_all_sources(settings: Settings | None = None) -> list[SourceConfig]:
    """Load all source configs from the sources directory."""
    if settings is None:
        settings = load_settings()
    sources_dir = PROJECT_ROOT / settings.sources_dir
    configs = []
    if sources_dir.is_dir():
        for p in sorted(sources_dir.glob("*.yaml")):
            configs.append(load_source_config(p))
    return configs
