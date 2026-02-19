"""Tests for ArcGIS source plugin with mocked HTTP."""

import responses
import pytest

from parcl.sources.arcgis_source import ArcGISSource, rings_to_wkt, geometry_to_wkt
from parcl.config import SourceConfig, FieldMapping, CrawlerConfig


def test_rings_to_wkt():
    rings = [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]
    result = rings_to_wkt(rings)
    assert result == "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))"


def test_geometry_to_wkt_point():
    geom = {"x": -97.7431, "y": 30.2672}
    result = geometry_to_wkt(geom)
    assert result == "POINT(-97.7431 30.2672)"


def test_geometry_to_wkt_none():
    assert geometry_to_wkt(None) == ""
    assert geometry_to_wkt({}) == ""


@responses.activate
def test_arcgis_fetch_single_layer(sample_crawler_config):
    config = SourceConfig(
        id="test_arcgis",
        source_type="arcgis",
        target_table="zoning_overlays",
        base_url="https://maps.example.com/MapServer",
        layers=[{"id": 0, "name": "TestLayer"}],
        field_map=[
            FieldMapping("NAME", "overlay_name", "text", False),
        ],
    )

    url = "https://maps.example.com/MapServer/0/query"
    features = [
        {"attributes": {"OBJECTID": 1, "NAME": "Zone A"}, "geometry": {"x": -97.7, "y": 30.2}},
        {"attributes": {"OBJECTID": 2, "NAME": "Zone B"}, "geometry": {"x": -97.8, "y": 30.3}},
    ]
    responses.add(
        responses.GET, url,
        json={"features": features, "exceededTransferLimit": False},
        status=200,
    )

    source = ArcGISSource(config, sample_crawler_config)
    batches = list(source.fetch())
    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert batches[0][0]["NAME"] == "Zone A"
    assert batches[0][0]["_geometry_wkt"] == "POINT(-97.7 30.2)"
    assert batches[0][0]["_layer_name"] == "TestLayer"
