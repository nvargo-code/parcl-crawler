"""Structured logging setup for parcl-crawler."""

import logging
import json
import sys
from datetime import datetime, timezone


class StructuredFormatter(logging.Formatter):
    """JSON-lines structured log formatter."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        # Merge any extra fields
        for key in ("source_id", "table", "rows", "duration_s"):
            if hasattr(record, key):
                entry[key] = getattr(record, key)
        return json.dumps(entry)


class SimpleFormatter(logging.Formatter):
    """Human-readable log formatter."""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )


def setup_logging(level: str = "INFO", fmt: str = "structured") -> None:
    """Configure root logger for parcl."""
    root = logging.getLogger("parcl")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    if fmt == "structured":
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(SimpleFormatter())
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the parcl namespace."""
    return logging.getLogger(f"parcl.{name}")
