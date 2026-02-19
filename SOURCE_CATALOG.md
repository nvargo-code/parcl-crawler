# Source Catalog — parcl-crawler Austin v1

## Socrata Sources (data.austintexas.gov)

| ID | Dataset | Resource ID | Format | Target Table | License | Refresh |
|----|---------|-------------|--------|--------------|---------|---------|
| `austin_permits` | Issued Construction Permits | `3syk-w9eu` | JSON API | `permits` | Public Domain | Daily |
| `austin_zoning_by_address` | Zoning by Address | `nbzi-qabm` | JSON API | `parcels` | Public Domain | As needed |
| `austin_zoning_cases` | Zoning Cases | `edir-dcnf` | JSON API | `zoning_cases` | Public Domain | Daily |
| `austin_boa_cases` | Board of Adjustment Cases | `ykxk-t5y9` | JSON API | `boa_cases` | Public Domain | Daily |
| `austin_water_treated` | Water Treated by Plant | `xtim-9ehs` | JSON API | `utility_capacity` | Public Domain | Annual |
| `austin_wastewater_treated` | Wastewater Treated by Plant | `vuwy-s6qv` | JSON API | `utility_capacity` | Public Domain | Annual |
| `austin_brownfields` | Brownfield Site List | `22wq-47zy` | JSON API | `environmental_constraints` | Public Domain | As needed |
| `austin_floodplain` | Fully Developed Floodplain | `2xn4-j3u2` | JSON API | `environmental_constraints` | Public Domain | Monthly |

### Socrata API Notes
- Base URL: `https://data.austintexas.gov/resource/{resource_id}.json`
- Pagination: `$limit` + `$offset` params
- Rate limit: 1,000 requests/hour without token, higher with `X-App-Token`
- All data is public domain

## ArcGIS REST Sources

| ID | Service | Endpoint | Target Table | License | Refresh |
|----|---------|----------|--------------|---------|---------|
| `arcgis_zoning_overlays` | Zoning Overlays (32 layers) | `maps.austintexas.gov/.../Zoning_2/MapServer` | `zoning_overlays` | Public Domain | Weekly |
| `arcgis_zoning_infill` | Zoning Infill Options (11 layers) | `maps.austintexas.gov/.../Zoning_4/MapServer` | `zoning_overlays` | Public Domain | Weekly |
| `arcgis_water_service` | Austin Water Service Area | `maps.austintexas.gov/.../AustinWater/MapServer` | `utility_capacity` | Public Domain | Monthly |
| `arcgis_travis_flood_zone` | Travis County Flood Zone (FEMA) | `services1.arcgis.com/.../Travis_County_Flood_Zone_Map_WFL1/FeatureServer` | `environmental_constraints` | Public Domain | Monthly |
| `arcgis_epa_brownfields` | EPA Brownfields (ACRES) | `geodata.epa.gov/.../FRS_INTERESTS/MapServer/0` | `environmental_constraints` | Public Domain | Quarterly |

### ArcGIS API Notes
- Query endpoint: `{service_url}/{layer_id}/query`
- Pagination: `resultOffset` + `resultRecordCount` params
- Geometry returned as ArcGIS rings/points, converted to WKT by crawler
- No API key required for public services
