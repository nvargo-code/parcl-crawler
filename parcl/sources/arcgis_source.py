"""ArcGIS REST API source plugin with multi-layer and geometry support."""

from __future__ import annotations

from typing import Any, Iterator

from parcl.config import CrawlerConfig, SourceConfig
from parcl.sources import register
from parcl.sources.base import BaseSource


def rings_to_wkt(rings: list[list[list[float]]]) -> str:
    """Convert ArcGIS ring geometry to WKT POLYGON."""
    if not rings:
        return ""
    parts = []
    for ring in rings:
        coords = ", ".join(f"{pt[0]} {pt[1]}" for pt in ring)
        parts.append(f"({coords})")
    return f"POLYGON({', '.join(parts)})"


def geometry_to_wkt(geom: dict[str, Any] | None) -> str:
    """Convert ArcGIS geometry object to WKT string."""
    if not geom:
        return ""
    if "rings" in geom:
        return rings_to_wkt(geom["rings"])
    if "x" in geom and "y" in geom:
        return f"POINT({geom['x']} {geom['y']})"
    if "paths" in geom:
        paths = geom["paths"]
        if paths:
            coords = ", ".join(f"{pt[0]} {pt[1]}" for pt in paths[0])
            return f"LINESTRING({coords})"
    return ""


@register("arcgis")
class ArcGISSource(BaseSource):
    """Fetches features from ArcGIS REST MapServer/FeatureServer layers."""

    def fetch(self) -> Iterator[list[dict[str, Any]]]:
        layers = self.config.layers or [{"id": 0, "name": "default"}]

        for layer_def in layers:
            layer_id = layer_def.get("id", 0)
            layer_name = layer_def.get("name", f"layer_{layer_id}")
            yield from self._fetch_layer(layer_id, layer_name)

    def _fetch_layer(
        self, layer_id: int, layer_name: str
    ) -> Iterator[list[dict[str, Any]]]:
        base = self.config.base_url.rstrip("/")
        url = f"{base}/{layer_id}/query"

        offset = 0
        limit = self.crawler.page_size
        use_pagination = True

        for page_num in range(self.crawler.max_pages):
            params = {
                "where": self.config.filters.get("where", "1=1"),
                "outFields": self.config.filters.get("outFields", "*"),
                "returnGeometry": "true",
                "f": "json",
            }
            if use_pagination:
                params["resultOffset"] = offset
                params["resultRecordCount"] = limit

            self.log.info(
                f"Layer {layer_name} ({layer_id}): page {page_num + 1}, offset={offset}"
            )
            resp = self.session.get(
                url, params=params, timeout=self.crawler.timeout_seconds
            )
            resp.raise_for_status()
            data = resp.json()

            # Handle ArcGIS error body (HTTP 200 with {"error": {...}})
            if "error" in data:
                err_msg = data["error"].get("message", "")
                if use_pagination and "pagination" in err_msg.lower():
                    self.log.info(
                        f"Layer {layer_name} ({layer_id}): pagination not supported, retrying without offset"
                    )
                    use_pagination = False
                    continue
                self.log.warning(
                    f"Layer {layer_name} ({layer_id}): ArcGIS error: {err_msg}"
                )
                break

            features = data.get("features", [])
            if not features:
                break

            # Flatten: merge attributes + geometry WKT + layer metadata
            records = []
            for feat in features:
                rec = dict(feat.get("attributes", {}))
                rec["_geometry_wkt"] = geometry_to_wkt(feat.get("geometry"))
                rec["_layer_id"] = layer_id
                rec["_layer_name"] = layer_name
                records.append(rec)

            yield records
            offset += limit

            # Non-paginated layers are always single-shot
            if not use_pagination:
                break

            # Check if server says there's more
            if not data.get("exceededTransferLimit", False) and len(features) < limit:
                break

            self._rate_limit()
