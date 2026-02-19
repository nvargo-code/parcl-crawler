# parcl-crawler

A parcel-anchored real estate data crawler for Austin TX / Travis County. Collects permits, zoning, utility capacity, ownership, and environmental constraint data from public APIs into a local DuckDB database.

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Initialize database
parcl init

# Run a single source
parcl run austin_permits

# Run all 13 sources
parcl run --all

# Check what's loaded
parcl db --info

# Get risk profile for an address
parcl profile "600 Congress Ave, Austin TX"

# Export for analytics
parcl export --format csv
parcl export --format parquet
parcl export --format jsonl
```

## What It Does

parcl-crawler collects and normalizes public real estate data from:

- **Permits** — Construction permits from Austin's open data portal
- **Zoning** — Base zoning, overlays, rezoning cases, variance cases
- **Utilities** — Water and wastewater treatment capacity
- **Environmental** — Flood zones (FEMA), brownfield sites, EPA data
- **Rights** — Deed restrictions, easements (stub for future)

All data is stored in a local DuckDB database with a unified schema. Each record includes the full raw API response for audit purposes.

## Data Sources

See [SOURCE_CATALOG.md](SOURCE_CATALOG.md) for the complete list of 13 data sources with URLs, formats, and refresh cadences.

## Commands

| Command | Description |
|---------|-------------|
| `parcl init` | Create database tables and views |
| `parcl run <source_id>` | Run ETL for one source |
| `parcl run --all` | Run ETL for all sources |
| `parcl list-sources` | Show sources and last run status |
| `parcl profile "<address>"` | Get risk profile for a parcel |
| `parcl export --format csv` | Export to CSV |
| `parcl export --format parquet` | Export to Parquet |
| `parcl export --format jsonl` | Export to JSONL |
| `parcl db --info` | Show table row counts |

## Configuration

- `config/settings.yaml` — Database type, logging, rate limits
- `config/sources/*.yaml` — One file per data source with field mappings
- `.env` — Optional API tokens (copy from `.env.example`)

### Switching to PostgreSQL

Edit `config/settings.yaml`:

```yaml
database:
  type: postgresql
  postgres_url: postgresql://user:pass@host:5432/parcl
```

## Adding a New City

1. Create YAML configs in `config/sources/` for each data source
2. Add a jurisdiction entry (the crawler seeds Austin by default)
3. Run `parcl run --all`

No code changes needed — the crawler is config-driven.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## Project Structure

```
parcl-crawler/
├── config/          # YAML configs (settings + per-source)
├── sql/             # Schema DDL and views
├── parcl/           # Python package
│   ├── sources/     # Source plugins (Socrata, ArcGIS, CSV, PDF)
│   └── etl/         # Extract → Transform → Load pipeline
├── tests/           # Pytest suite
└── scripts/         # Utility scripts
```
