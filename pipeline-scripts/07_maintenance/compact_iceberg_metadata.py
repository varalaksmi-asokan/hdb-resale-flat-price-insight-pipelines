
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "05_ETL"))

from common import _BOTO3_SESSION, _drop_iceberg_table, execute_athena_sql
from config import GLUE_DATABASE, TABLES

METADATA_TABLE_NAMES = ["metadata_tables", "table_parameters", "table_watermarks", "pipeline_runs"]

KNOWN_NON_ICEBERG_PREFIXES = {
    "athena-results",
    "profiling_reports",
    "raw",
    "cleaned",
    "transformed",
    "hashed",
    "failed",
    "audit",
    # Written by the Lambda's _persist_run_report/_save_alert_log (success and
    # failure run logs). It's not an Iceberg table, so the orphan sweep below
    # was treating it as an orphan and deleting it moments after every run's
    # RecordRunSummary step wrote to it - which is why alert-logs/ was always
    # empty by the time anyone checked.
    "alert-logs",
}

def all_table_names() -> list:
    data_table_names = [real_name for real_name, _location in TABLES.values()]
    return list(dict.fromkeys(data_table_names + METADATA_TABLE_NAMES))

def fetch_live_catalog_buckets() -> dict:
    glue = _BOTO3_SESSION.client("glue")
    buckets: dict = {}
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=GLUE_DATABASE):
        for table in page.get("TableList", []):
            name = table["Name"]
            location = table.get("StorageDescriptor", {}).get("Location", "") or ""
            if not location.startswith("s3://"):
                continue
            bucket, _, key = location[len("s3://"):].partition("/")
            top_level = key.split("/")[0] if key else ""
            if not top_level:
                continue
            buckets.setdefault(bucket, {})[top_level] = name
    return buckets

def sweep_orphans(dry_run: bool) -> None:
    print("=" * 70)
    print("PHASE 1: orphan sweep (live Glue Catalog ground truth)")
    print("=" * 70)
    buckets = fetch_live_catalog_buckets()
    total_tables = sum(len(v) for v in buckets.values())
    print(f"Live catalog: {total_tables} table(s) across {len(buckets)} bucket(s)")

    s3 = _BOTO3_SESSION.client("s3")
    found_any_orphan = False
    for bucket, valid_prefixes in sorted(buckets.items()):
        actual_prefixes = set()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                actual_prefixes.add(common_prefix["Prefix"].rstrip("/"))

        orphans = sorted(actual_prefixes - set(valid_prefixes) - KNOWN_NON_ICEBERG_PREFIXES)
        if not orphans:
            print(f"  {bucket}: clean ({len(valid_prefixes)} valid prefix(es), no orphans)")
            continue

        found_any_orphan = True
        for prefix in orphans:
            location = f"s3://{bucket}/{prefix}/"
            # DELETION DISABLED 2026-09-13: this used to call _drop_iceberg_table()
            # here (live, no confirmation) and deleted real tables/prefixes it
            # wrongly flagged as orphans - including alert-logs/ despite the
            # KNOWN_NON_ICEBERG_PREFIXES protection above, and, separately, real
            # data during the incident investigated that day. Orphan detection
            # is still useful as a report, so it stays - just log-only now.
            # Review each line below manually; do not re-enable automatic
            # deletion without first fixing whatever made these false positives.
            print(f"  [ORPHAN - not deleted, review manually]: {location}")

    if not found_any_orphan:
        print("No orphans found anywhere - every bucket matches the live Glue Catalog exactly.")
    print()

def run_vacuum(tables: list, max_snapshot_age_seconds, dry_run: bool) -> None:
    print("=" * 70)
    print("PHASE 2: metadata compaction (VACUUM)")
    print("=" * 70)
    print(f"Database   : {GLUE_DATABASE}")
    print(f"Tables     : {len(tables)} -> {', '.join(tables)}")
    if max_snapshot_age_seconds is not None:
        print(f"Retention  : lowering vacuum_max_snapshot_age_seconds to {max_snapshot_age_seconds}s first")
    print(f"Dry run    : {dry_run}")
    print("-" * 70)

    for table in tables:
        if max_snapshot_age_seconds is not None:
            alter_sql = (
                f'ALTER TABLE "{table}" SET PROPERTIES '
                f"vacuum_max_snapshot_age_seconds='{max_snapshot_age_seconds}'"
            )
            if dry_run:
                print(f"[dry-run] {alter_sql}")
            else:
                try:
                    execute_athena_sql(alter_sql, f"lower retention window on {table}")
                except Exception as exc:
                    print(f"ALTER FAIL - {table}: {exc}")

        vacuum_sql = f"VACUUM {table}"
        if dry_run:
            print(f"[dry-run] {vacuum_sql}")
            continue
        try:
            execute_athena_sql(vacuum_sql, f"VACUUM {table}")
            print(f"OK   - {table}")
        except Exception as exc:
            print(f"FAIL - {table}: {exc}")

    print("-" * 70)
    print("Done. Metadata cleaned up where eligible - data files were not rewritten.")

def main():
    parser = argparse.ArgumentParser(
        description="Pre-flight pipeline hygiene: orphan S3 sweep + Iceberg VACUUM."
    )
    parser.add_argument("--only", nargs="+", default=None,
                         help="Limit VACUUM to these table names instead of every table.")
    parser.add_argument("--max-snapshot-age-seconds", type=int, default=None,
                         help="Lower vacuum_max_snapshot_age_seconds before vacuuming (e.g. 120 = 2 min).")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, change nothing.")
    parser.add_argument("--skip-orphan-sweep", action="store_true", help="VACUUM only.")
    parser.add_argument("--skip-vacuum", action="store_true", help="Orphan sweep only.")
    args, _unrecognized = parser.parse_known_args()

    if not args.skip_orphan_sweep:
        sweep_orphans(dry_run=args.dry_run)
    if not args.skip_vacuum:
        tables = args.only if args.only else all_table_names()
        run_vacuum(tables, args.max_snapshot_age_seconds, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
