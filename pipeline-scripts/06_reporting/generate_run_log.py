
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "05_ETL"))

import boto3
import pandas as pd

from common import athena_read_sql
from config import (
    AWS_REGION,
    COLLECTION_ID,
    DATE_RANGE_START,
    DATE_RANGE_END,
)

BAR = "=" * 70
SEP = "-" * 70

STAGES = [
    {"key": "ingestion_to_source", "label": "STAGE 1",  "title": "ingestion_to_source (job_1)",
     "previous": None, "failed_tag": None},
    {"key": "raw_iceberg", "label": "STAGE 2", "title": "raw_iceberg (job_2)",
     "previous": "ingestion_to_source", "failed_tag": "raw"},
    {"key": "profiling", "label": "STAGE 2b", "title": "profiling (job_2b)",
     "previous": None, "failed_tag": None, "report_only": True},
    {"key": "cleaned_iceberg", "label": "STAGE 3", "title": "cleaned_iceberg (job_3)",
     "previous": "raw_iceberg", "failed_tag": "cleaned"},
    {"key": "transformed_iceberg", "label": "STAGE 4", "title": "transformed_iceberg (job_4)",
     "previous": "cleaned_iceberg", "failed_tag": "transformed"},
    {"key": "hashed_iceberg", "label": "STAGE 5", "title": "hashed_iceberg (job_5)",
     "previous": "transformed_iceberg", "failed_tag": "hashed"},
]

CHAIN_SHORT_LABEL = {
    "ingestion_to_source": "source",
    "raw_iceberg": "raw",
    "cleaned_iceberg": "cleaned",
    "transformed_iceberg": "transformed",
    "hashed_iceberg": "hashed",
}

def _int_or_zero(value) -> int:
    return 0 if pd.isna(value) else int(value)

def latest_run(layer: str):
    df = athena_read_sql(f"""
        SELECT run_id, start_time, end_time, status, number_of_records, error_message
        FROM pipeline_runs
        WHERE layer = '{layer}'
        ORDER BY run_id DESC
        LIMIT 1
    """)
    return None if df.empty else df.iloc[0]

def reject_breakdown(failed_tag: str, start_time, end_time):
    window_start = (pd.Timestamp(start_time) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    window_end = (pd.Timestamp(end_time) + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    df = athena_read_sql(f"""
        SELECT _failure_reason AS reason, COUNT(*) AS cnt
        FROM failed_iceberg
        WHERE _failed_stage = '{failed_tag}'
          AND from_iso8601_timestamp(_failed_at)
              BETWEEN TIMESTAMP '{window_start}' AND TIMESTAMP '{window_end}'
        GROUP BY _failure_reason
        ORDER BY cnt DESC
    """)
    if df.empty:
        return []
    return list(zip(df["reason"].tolist(), df["cnt"].astype(int).tolist()))

def build_report() -> tuple[str, bool]:
    lines = []
    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]

    lines.append(BAR)
    lines.append("HDB RESALE FLAT PRICES PIPELINE - RUN LOG & COUNT RECONCILIATION")
    lines.append(BAR)
    lines.append(f"Report run at : {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    lines.append(f"Environment   : AWS Account {account_id}, {AWS_REGION}")
    lines.append(f"Source        : data.gov.sg collection {COLLECTION_ID} (Resale Flat Prices)")
    lines.append(f"Date scope    : {DATE_RANGE_START} to {DATE_RANGE_END}")
    lines.append(SEP)

    stage_counts = {}
    rejected_counts = {}
    stopped_at = None

    for stage in STAGES:
        run = latest_run(stage["key"])
        lines.append(f"{stage['label']} - {stage['title']}")
        lines.append(SEP)

        if run is None:
            lines.append("status         : NOT YET RUN (no pipeline_runs row found for this layer)")
            lines.append(SEP)
            lines.append("")
            stopped_at = stage["title"]
            lines.append(f"STOPPED - {stage['title']} has not run yet; nothing downstream can be verified.")
            break

        run_id = int(run["run_id"])
        status = str(run["status"])
        count = _int_or_zero(run["number_of_records"])
        lines.append(f"run_id         : {run_id}")
        lines.append(f"start_time     : {run['start_time']}")
        lines.append(f"end_time       : {run['end_time']}")
        lines.append(f"status         : {status}")

        if stage.get("report_only"):
            lines.append(f"rows_profiled  : {count:,}")
            lines.append("note           : report-only stage, no row count to reconcile")
            lines.append(SEP)
            lines.append("")
            if status != "SUCCEEDED":
                stopped_at = stage["title"]
                lines.append(f"STOPPED - {stage['title']} status is {status}, not SUCCEEDED.")
                break
            continue

        if stage["previous"] is None:
            lines.append(f"rows_landed    : {count:,}")
            lines.append(f"detail         : {run['error_message']}")
            lines.append(SEP)
            lines.append("")
            stage_counts[stage["key"]] = count
            if status != "SUCCEEDED":
                stopped_at = stage["title"]
                lines.append(f"STOPPED - {stage['title']} status is {status}, not SUCCEEDED.")
                break
            continue

        expected = stage_counts.get(stage["previous"])
        rejects = reject_breakdown(stage["failed_tag"], run["start_time"], run["end_time"])
        rejected_total = sum(c for _, c in rejects)

        lines.append(f"rows_kept      : {count:,}")
        lines.append(f"rows_rejected  : {rejected_total:,}")

        if expected is None:
            reconciled = False
            lines.append(f"reconciliation : CANNOT VERIFY - no recorded count for {stage['previous']}")
        elif status == "SUCCEEDED" and (count + rejected_total == expected):
            reconciled = True
            lines.append(f"reconciliation : {count:,} + {rejected_total:,} = {expected:,} "
                          f"- MATCHES {stage['previous']} OK")
        else:
            reconciled = False
            lines.append(f"reconciliation : {count:,} + {rejected_total:,} = {count + rejected_total:,} "
                          f"vs expected {expected:,} (from {stage['previous']}) - MISMATCH")

        if rejects:
            lines.append("reject_breakdown:")
            for reason, c in rejects:
                pct = (c / rejected_total * 100) if rejected_total else 0.0
                lines.append(f"   {reason:<30} : {c:>7,}  ({pct:4.1f}%)")

        lines.append(SEP)
        lines.append("")

        stage_counts[stage["key"]] = count
        rejected_counts[stage["key"]] = rejected_total

        if not reconciled:
            stopped_at = stage["title"]
            lines.append(f"STOPPED - {stage['title']} did not reconcile cleanly (status={status}). "
                          f"Not evaluating any stage after this one.")
            break

    all_reconciled = stopped_at is None

    if all_reconciled:
        lines.append(BAR)
        lines.append("END-TO-END COUNT CHAIN")
        lines.append(BAR)
        chain = " -> ".join(
            f"{stage_counts[key]:,} ({label})" for key, label in CHAIN_SHORT_LABEL.items()
        )
        lines.append(chain)
        source_total = stage_counts["ingestion_to_source"]
        final_total = stage_counts["hashed_iceberg"]
        quarantined_total = source_total - final_total
        lines.append(f"Total rows reconciled       : {source_total:,}")
        lines.append(f"Total rows retained (final) : {final_total:,}  "
                      f"({final_total / source_total * 100:.1f}%)")
        lines.append(f"Total rows quarantined      : {quarantined_total:,}  "
                      f"({quarantined_total / source_total * 100:.1f}%)")
        for stage in STAGES:
            if stage["key"] in rejected_counts and rejected_counts[stage["key"]] > 0:
                lines.append(f"   - {stage['title']:<28} : {rejected_counts[stage['key']]:,}")
        lines.append("Every stage transition reconciled with ZERO unexplained loss.")
    else:
        lines.append(BAR)
        lines.append(f"RESULT: NOT FULLY RECONCILED - stopped at {stopped_at}")
        lines.append(BAR)

    lines.append(BAR)
    return "\n".join(lines), all_reconciled

def main():
    parser = argparse.ArgumentParser(description="Generate the HDB pipeline run-log / reconciliation report.")
    parser.add_argument("--output", default=None,
                         help="Write the report to this file too (default: run_log_<UTC timestamp>.txt "
                              "next to this script)")
    args = parser.parse_args()

    try:
        report_text, all_reconciled = build_report()
    except Exception as exc:
        print(f"ERROR - could not generate the run log: {exc}", file=sys.stderr)
        sys.exit(2)

    print(report_text)

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent / f"run_log_{datetime.utcnow():%Y%m%dT%H%M%SZ}.txt"
    )
    output_path.write_text(report_text + "\n")
    print(f"\n(written to {output_path})")

    sys.exit(0 if all_reconciled else 1)

if __name__ == "__main__":
    main()
