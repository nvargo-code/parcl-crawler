"""Tests for config loading."""

import tempfile
from pathlib import Path

import yaml
import pytest

from parcl.config import load_source_config, SourceConfig


def test_load_source_config_from_yaml(tmp_path):
    config = {
        "id": "test_source",
        "source_type": "socrata",
        "target_table": "permits",
        "base_url": "https://example.com",
        "dataset_id": "abc-123",
        "field_map": [
            {"raw_field": "col_a", "schema_field": "field_a", "type": "text", "required": True},
        ],
    }
    path = tmp_path / "test.yaml"
    path.write_text(yaml.dump(config))

    result = load_source_config(path)
    assert result.id == "test_source"
    assert result.source_type == "socrata"
    assert len(result.field_map) == 1
    assert result.field_map[0].raw_field == "col_a"
    assert result.field_map[0].required is True


def test_source_config_defaults():
    sc = SourceConfig(id="x", source_type="csv", target_table="permits")
    assert sc.jurisdiction_id == "austin-tx"
    assert sc.filters == {}
    assert sc.field_map == []
