"""Click CLI for parcl-crawler."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta

import click

from parcl.config import load_all_sources, load_settings, load_source_config, PROJECT_ROOT
from parcl.db import create_database, init_schema
from parcl.logger import setup_logging, get_logger

# Hours per cadence string
_CADENCE_HOURS: dict[str, int] = {
    "hourly": 1,
    "daily": 24,
    "weekly": 168,
    "monthly": 720,
}


def _is_fresh(db, source_id: str, cadence: str | None) -> bool:
    """Return True if the source was run within its refresh cadence."""
    row = db.fetchone(
        "SELECT last_run_at, refresh_cadence FROM sources WHERE id = ?",
        (source_id,),
    )
    if not row or not row[0]:
        return False
    last_run = row[0]
    if isinstance(last_run, str):
        last_run = datetime.fromisoformat(last_run)
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)
    cadence_str = row[1] or cadence or "daily"
    hours = _CADENCE_HOURS.get(cadence_str, 24)
    return datetime.now(timezone.utc) - last_run < timedelta(hours=hours)

log = get_logger("cli")


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def main(verbose: bool) -> None:
    """parcl-crawler: Parcel-anchored real estate data crawler."""
    settings = load_settings()
    level = "DEBUG" if verbose else settings.logging_level
    setup_logging(level, settings.logging_format)


@main.command()
def init() -> None:
    """Initialize the database schema (create all tables and views)."""
    settings = load_settings()
    db = create_database(settings.database)
    init_schema(db)
    click.echo("Database initialized successfully.")
    counts = db.table_row_counts()
    for table, count in counts.items():
        click.echo(f"  {table}: {count} rows")
    db.close()


@main.command()
@click.argument("source_id", required=False)
@click.option("--all", "run_all", is_flag=True, help="Run all configured sources")
@click.option(
    "--skip-fresh",
    is_flag=True,
    help="Skip sources that were run within their refresh cadence",
)
def run(source_id: str | None, run_all: bool, skip_fresh: bool) -> None:
    """Run ETL for a specific source or all sources."""
    from parcl.etl.pipeline import run_source

    settings = load_settings()
    db = create_database(settings.database)

    if run_all:
        sources = load_all_sources(settings)
        if not sources:
            click.echo("No source configs found.", err=True)
            db.close()
            sys.exit(1)
        click.echo(f"Running {len(sources)} sources...")
        for src in sources:
            if skip_fresh and _is_fresh(db, src.id, src.refresh_cadence):
                click.echo(f"  {src.id}: skipped (within {src.refresh_cadence} cadence)")
                continue
            try:
                summary = run_source(src, db)
                click.echo(
                    f"  {src.id}: {summary['loaded_records']} records "
                    f"in {summary['duration_seconds']}s"
                )
            except Exception as e:
                click.echo(f"  {src.id}: ERROR - {e}", err=True)
    elif source_id:
        # Find the source config
        sources_dir = PROJECT_ROOT / settings.sources_dir
        config_path = sources_dir / f"{source_id}.yaml"
        if not config_path.exists():
            click.echo(f"Source config not found: {config_path}", err=True)
            db.close()
            sys.exit(1)
        src = load_source_config(config_path)
        summary = run_source(src, db)
        click.echo(json.dumps(summary, indent=2))
    else:
        click.echo("Specify a source_id or use --all", err=True)
        sys.exit(1)

    db.close()


@main.command("list-sources")
def list_sources() -> None:
    """List all configured data sources and their last run status."""
    settings = load_settings()
    db = create_database(settings.database)
    sources = load_all_sources(settings)

    click.echo(f"{'ID':<35} {'Type':<10} {'Table':<25} {'Last Run':<22} {'Rows':<8}")
    click.echo("-" * 100)

    for src in sources:
        row = db.fetchone(
            "SELECT last_run_at, last_row_count FROM sources WHERE id = ?",
            (src.id,),
        )
        last_run = row[0] if row and row[0] else "never"
        row_count = row[1] if row and row[1] else 0
        click.echo(
            f"{src.id:<35} {src.source_type:<10} {src.target_table:<25} "
            f"{str(last_run):<22} {row_count:<8}"
        )
    db.close()


@main.command()
@click.argument("query")
@click.option("--output", "-o", type=click.Choice(["pretty", "json"]), default="pretty")
def profile(query: str, output: str) -> None:
    """Get risk profile for a parcel by address."""
    from parcl.profile import get_parcel_risk_profile

    settings = load_settings()
    db = create_database(settings.database)
    result = get_parcel_risk_profile(query, db)
    db.close()

    if output == "json":
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        click.echo(f"\nParcel Risk Profile: {result.get('query', query)}")
        click.echo(f"Matched: {result.get('matched_address', 'N/A')}")
        click.echo(f"Zoning: {json.dumps(result.get('zoning', {}), indent=2)}")
        click.echo(f"\nRisks ({len(result.get('risks', []))}):")
        for risk in result.get("risks", []):
            click.echo(f"  [{risk['severity'].upper()}] {risk['type']}: {risk['label']}")
        click.echo(f"\nPermits ({len(result.get('permits', []))}):")
        for permit in result.get("permits", [])[:5]:
            click.echo(
                f"  {permit.get('permit_number', 'N/A')} - {permit.get('type', 'N/A')} "
                f"(${permit.get('valuation', 0):,.0f})"
            )
        facts = result.get("supporting_facts", {})
        click.echo(f"\nFacts: {json.dumps(facts, indent=2)}")
        click.echo(f"Sources: {', '.join(result.get('data_sources', []))}")
        if result.get("warnings"):
            click.echo(f"\nWarnings: {result['warnings']}")


@main.command()
@click.option("--format", "fmt", type=click.Choice(["csv", "parquet", "jsonl"]), default="csv")
@click.option("--output-dir", "-o", default=None, help="Output directory")
def export(fmt: str, output_dir: str | None) -> None:
    """Export parcel_features view to CSV, Parquet, or JSONL."""
    from parcl.exporter import export_data

    settings = load_settings()
    db = create_database(settings.database)
    out_dir = output_dir or settings.export_output_dir
    path = export_data(db, fmt, out_dir)
    db.close()
    click.echo(f"Exported to: {path}")


@main.command("db")
@click.option("--info", is_flag=True, help="Show table row counts")
def db_info(info: bool) -> None:
    """Database utilities."""
    if info:
        settings = load_settings()
        db = create_database(settings.database)
        counts = db.table_row_counts()
        click.echo(f"{'Table':<30} {'Rows':<10}")
        click.echo("-" * 40)
        total = 0
        for table, count in counts.items():
            click.echo(f"{table:<30} {count:<10}")
            if count > 0:
                total += count
        click.echo("-" * 40)
        click.echo(f"{'Total':<30} {total:<10}")
        db.close()
    else:
        click.echo("Use --info to show table row counts")


if __name__ == "__main__":
    main()
