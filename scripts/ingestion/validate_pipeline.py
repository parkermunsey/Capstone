from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*args, **kwargs):
        return False

try:
    from sqlalchemy import create_engine, text
except ModuleNotFoundError:
    create_engine = None

    def text(sql):
        return sql


EXPECTED_SOURCES = ["adea", "caras", "craigslist", "mha", "mpm", "plum"]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
VIEW_SQL_PATH = Path(__file__).resolve().with_name("dashboard_ready_listings.sql")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate recent raw -> staging -> dashboard pipeline behavior.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
        help="Only validate rows newer than this many hours. Default: 24.",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=EXPECTED_SOURCES,
        help="Sources expected in the recent validation window.",
    )
    parser.add_argument(
        "--apply-dashboard-view",
        action="store_true",
        help="Create or replace the dashboard_ready_listings view before validation.",
    )
    return parser.parse_args()


def print_section(title: str):
    print(f"\n== {title} ==")


def as_dict(rows, key_name: str, value_name: str) -> dict[str, int]:
    return {row[key_name]: row[value_name] for row in rows}


def relation_exists(conn, relation_name: str) -> bool:
    return bool(
        conn.execute(
            text("SELECT to_regclass(:relation_name)"),
            {"relation_name": relation_name},
        ).scalar_one()
    )


def apply_dashboard_view(engine, failures: list[str]) -> bool:
    """
    Apply the dashboard view in its own transaction so a failure here
    does not poison the later validation transaction.
    """
    print_section("Apply Dashboard View")

    try:
        sql_text = VIEW_SQL_PATH.read_text(encoding="utf-8")
    except Exception as exc:
        failures.append(f"could not read {VIEW_SQL_PATH.name}. Error: {exc}")
        return False

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(sql_text)
        print(f"Applied dashboard view SQL from {VIEW_SQL_PATH.name}")
        return True
    except Exception as exc:
        failures.append(
            "could not create or replace dashboard_ready_listings. "
            "Check CREATE VIEW permission, confirm stg_listings exists, and confirm the SQL view "
            "matches the current stg_listings schema. "
            f"Database error: {exc}"
        )
        return False


def validate_raw_ingestion(conn, cutoff: datetime, required_sources: list[str], failures: list[str]):
    print_section("Raw Ingestion")

    rows = conn.execute(
        text(
            """
            SELECT source, COUNT(*) AS row_count
            FROM raw_listings
            WHERE scraped_at >= :cutoff
            GROUP BY source
            ORDER BY source
            """
        ),
        {"cutoff": cutoff},
    ).mappings().all()

    counts = as_dict(rows, "source", "row_count")
    total_recent_rows = sum(counts.get(source, 0) for source in required_sources)

    if total_recent_rows == 0:
        failures.append(
            "no recent raw_listings rows were found in the validation window. "
            "Run the ingesters first or increase --lookback-hours."
        )

    for source in required_sources:
        print(f"{source}: raw rows={counts.get(source, 0)}")
        if counts.get(source, 0) == 0:
            failures.append(f"raw ingestion missing recent rows for source={source}")


def validate_staging(conn, cutoff: datetime, required_sources: list[str], failures: list[str]):
    print_section("Staging")

    counts = conn.execute(
        text(
            """
            SELECT source, COUNT(*) AS row_count
            FROM stg_listings
            WHERE observed_at >= :cutoff
            GROUP BY source
            ORDER BY source
            """
        ),
        {"cutoff": cutoff},
    ).mappings().all()

    count_map = as_dict(counts, "source", "row_count")
    total_recent_rows = sum(count_map.get(source, 0) for source in required_sources)

    if total_recent_rows == 0:
        failures.append(
            "no recent stg_listings rows were found in the validation window. "
            "Run normalize_raw_to_stg.py first or increase --lookback-hours."
        )

    quality_rows = conn.execute(
        text(
            """
            SELECT
                source,
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE source_record_id IS NULL) AS missing_source_record_id,
                COUNT(*) FILTER (WHERE listing_title IS NULL) AS missing_listing_title,
                COUNT(*) FILTER (WHERE listing_url IS NULL) AS missing_listing_url,
                COUNT(*) FILTER (WHERE cross_source_fingerprint IS NULL) AS missing_cross_source_fingerprint
            FROM stg_listings
            WHERE observed_at >= :cutoff
            GROUP BY source
            ORDER BY source
            """
        ),
        {"cutoff": cutoff},
    ).mappings().all()

    quality_map = {row["source"]: row for row in quality_rows}

    for source in required_sources:
        row_count = count_map.get(source, 0)
        print(f"{source}: stg rows={row_count}")

        if row_count == 0:
            failures.append(f"staging missing recent rows for source={source}")
            continue

        quality = quality_map.get(source)
        if quality is None:
            failures.append(f"staging quality checks could not load aggregates for source={source}")
            continue

        if quality["missing_source_record_id"] > 0:
            failures.append(f"staging has NULL source_record_id values for source={source}")
        if quality["missing_listing_title"] == quality["row_count"]:
            failures.append(f"staging has no listing_title values for source={source}")
        if source != "mha" and quality["missing_listing_url"] == quality["row_count"]:
            failures.append(f"staging has no listing_url values for source={source}")
        if quality["missing_cross_source_fingerprint"] > 0:
            failures.append(f"staging has NULL cross_source_fingerprint values for source={source}")


def validate_dedupe(conn, cutoff: datetime, failures: list[str], dashboard_view_ready: bool):
    print_section("Dedupe")

    duplicate_source_keys = conn.execute(
        text(
            """
            SELECT COUNT(*) AS duplicate_groups
            FROM (
                SELECT source, source_record_id
                FROM stg_listings
                WHERE observed_at >= :cutoff
                GROUP BY source, source_record_id
                HAVING COUNT(*) > 1
            ) grouped
            """
        ),
        {"cutoff": cutoff},
    ).scalar_one()

    print(f"duplicate source keys in stg_listings: {duplicate_source_keys}")

    if duplicate_source_keys != 0:
        failures.append("stg_listings has duplicate (source, source_record_id) groups in recent data")

    if not dashboard_view_ready:
        failures.append(
            "dashboard_ready_listings is unavailable, so dashboard dedupe validation was skipped. "
            "Re-run with --apply-dashboard-view or check dashboard_ready_listings.sql."
        )
        return

    dashboard_group_count = conn.execute(
        text(
            """
            SELECT COUNT(DISTINCT COALESCE(NULLIF(cross_source_fingerprint, ''), NULLIF(listing_fingerprint, ''), source || ':' || source_record_id))
            FROM stg_listings
            WHERE observed_at >= :cutoff
            """
        ),
        {"cutoff": cutoff},
    ).scalar_one()

    dashboard_view_count = conn.execute(
        text(
            """
            SELECT COUNT(*) AS row_count
            FROM dashboard_ready_listings
            WHERE observed_at >= :cutoff
            """
        ),
        {"cutoff": cutoff},
    ).scalar_one()

    print(f"expected dashboard groups from stg_listings: {dashboard_group_count}")
    print(f"rows in dashboard_ready_listings: {dashboard_view_count}")

    if dashboard_view_count != dashboard_group_count:
        failures.append(
            "dashboard_ready_listings row count does not match distinct dedupe groups from stg_listings"
        )


def validate_craigslist(conn, cutoff: datetime, failures: list[str], raw_ready: bool, stg_ready: bool):
    print_section("Craigslist")

    if not raw_ready:
        failures.append("craigslist raw validation was skipped because public.raw_listings is missing.")
    if not stg_ready:
        failures.append("craigslist staging validation was skipped because public.stg_listings is missing.")
    if not raw_ready or not stg_ready:
        return

    raw_count = conn.execute(
        text(
            """
            SELECT COUNT(*) AS row_count
            FROM raw_listings
            WHERE source = 'craigslist'
              AND scraped_at >= :cutoff
            """
        ),
        {"cutoff": cutoff},
    ).scalar_one()

    stg_row = conn.execute(
        text(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE listing_url IS NOT NULL) AS rows_with_url,
                COUNT(*) FILTER (WHERE rent_min IS NOT NULL OR rent_max IS NOT NULL) AS rows_with_rent
            FROM stg_listings
            WHERE source = 'craigslist'
              AND observed_at >= :cutoff
            """
        ),
        {"cutoff": cutoff},
    ).mappings().one()

    sample_rows = conn.execute(
        text(
            """
            SELECT source_record_id, listing_title, rent_min, listing_url
            FROM stg_listings
            WHERE source = 'craigslist'
              AND observed_at >= :cutoff
            ORDER BY observed_at DESC
            LIMIT 3
            """
        ),
        {"cutoff": cutoff},
    ).mappings().all()

    print(f"recent raw craigslist rows: {raw_count}")
    print(f"recent staged craigslist rows: {stg_row['row_count']}")
    print(f"recent staged craigslist rows with url: {stg_row['rows_with_url']}")
    print(f"recent staged craigslist rows with rent: {stg_row['rows_with_rent']}")

    for sample in sample_rows:
        print(
            "sample:",
            sample["source_record_id"],
            "|",
            sample["listing_title"],
            "| rent_min=",
            sample["rent_min"],
            "|",
            sample["listing_url"],
        )

    if raw_count == 0:
        failures.append("craigslist raw ingestion did not produce recent rows")
    if stg_row["row_count"] == 0:
        failures.append("craigslist normalization did not produce recent staged rows")
    if stg_row["rows_with_url"] == 0:
        failures.append("craigslist staged rows are missing listing_url values")


def validate_dashboard_view(conn, cutoff: datetime, failures: list[str]):
    print_section("Dashboard View")

    if not relation_exists(conn, "public.dashboard_ready_listings"):
        failures.append(
            "dashboard_ready_listings does not exist. Re-run with --apply-dashboard-view "
            "or check dashboard_ready_listings.sql."
        )
        return

    row_count = conn.execute(
        text(
            """
            SELECT COUNT(*) AS row_count
            FROM dashboard_ready_listings
            WHERE observed_at >= :cutoff
            """
        ),
        {"cutoff": cutoff},
    ).scalar_one()

    sample_rows = conn.execute(
        text(
            """
            SELECT
                source,
                listing_title,
                address,
                bedrooms,
                dashboard_rent,
                availability_status,
                is_currently_available,
                duplicate_count
            FROM dashboard_ready_listings
            WHERE observed_at >= :cutoff
            ORDER BY observed_at DESC
            LIMIT 5
            """
        ),
        {"cutoff": cutoff},
    ).mappings().all()

    print(f"recent dashboard rows: {row_count}")
    for sample in sample_rows:
        print(
            "sample:",
            sample["source"],
            "|",
            sample["listing_title"],
            "|",
            sample["address"],
            "| bedrooms=",
            sample["bedrooms"],
            "| rent=",
            sample["dashboard_rent"],
            "| availability=",
            sample["availability_status"],
            "| current=",
            sample["is_currently_available"],
            "| duplicate_count=",
            sample["duplicate_count"],
        )

    if row_count == 0:
        failures.append("dashboard_ready_listings returned no recent rows")


def main():
    args = parse_args()

    load_dotenv(PROJECT_ROOT / ".env")

    database_url = os.getenv("DATABASE_URL")

    if create_engine is None:
        print(
            "SQLAlchemy is not installed. Run `pip install -r requirements.txt` first.",
            file=sys.stderr,
        )
        sys.exit(2)

    if not database_url:
        raise ValueError("DATABASE_URL is not set in your .env file.")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.lookback_hours)
    failures: list[str] = []

    engine = create_engine(database_url, pool_pre_ping=True)

    try:
        if args.apply_dashboard_view:
            apply_dashboard_view(engine, failures)

        with engine.begin() as conn:
            raw_ready = relation_exists(conn, "public.raw_listings")
            stg_ready = relation_exists(conn, "public.stg_listings")
            dashboard_view_ready = relation_exists(conn, "public.dashboard_ready_listings")

            if not raw_ready:
                failures.append(
                    "public.raw_listings does not exist. Confirm the schema is loaded before live testing."
                )
            if not stg_ready:
                failures.append(
                    "public.stg_listings does not exist. Confirm the schema is loaded before normalization."
                )

            if raw_ready:
                validate_raw_ingestion(conn, cutoff, args.sources, failures)

            if stg_ready:
                validate_staging(conn, cutoff, args.sources, failures)
                validate_dedupe(conn, cutoff, failures, dashboard_view_ready)

            validate_craigslist(conn, cutoff, failures, raw_ready, stg_ready)

            if dashboard_view_ready:
                validate_dashboard_view(conn, cutoff, failures)
            else:
                failures.append(
                    "dashboard_ready_listings does not exist after validation startup. "
                    "Check dashboard_ready_listings.sql."
                )

    except Exception as exc:
        print(
            "\nValidation failed with a database exception. "
            "Common causes are missing SELECT permissions, missing tables, or schema drift. "
            f"Database error: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    print_section("Result")
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        sys.exit(1)

    print("Validation passed.")


if __name__ == "__main__":
    main()