
import datetime
from typing import Tuple

import pandas as pd

from common import get_logger, read_csv_files_from_s3, record_audit, route_to_failed, send_alert, write_by_load_type
from context_tracking import get_previous_stage_count, reconcile_count, record_stage_result
from config import (
    DATE_RANGE_END,
    DATE_RANGE_START,
    MAX_ROWS_TO_INGEST,
    SOURCE_MANUAL_UPLOAD_PREFIX,
    SOURCE_S3_BUCKET,
    SOURCE_S3_PREFIX,
)

logger = get_logger("job_2_raw_iceberg")

def read_source_files() -> Tuple[pd.DataFrame, int]:
    automated_df = read_csv_files_from_s3(SOURCE_S3_BUCKET, SOURCE_S3_PREFIX)
    logger.info("Read %d rows from automated source file(s) under s3://%s/%s/", len(automated_df), SOURCE_S3_BUCKET, SOURCE_S3_PREFIX)

    manual_df = read_csv_files_from_s3(SOURCE_S3_BUCKET, SOURCE_MANUAL_UPLOAD_PREFIX)
    if not manual_df.empty:
        logger.info("Read %d rows from manually-uploaded source file(s) under s3://%s/%s/", len(manual_df), SOURCE_S3_BUCKET, SOURCE_MANUAL_UPLOAD_PREFIX)

    combined = pd.concat([automated_df, manual_df], ignore_index=True) if not manual_df.empty else automated_df
    return combined, len(automated_df)

def filter_to_configured_date_range(df: pd.DataFrame) -> pd.DataFrame:
    month = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    start, end = pd.to_datetime(DATE_RANGE_START), pd.to_datetime(DATE_RANGE_END)
    in_range = month.notna() & (month >= start) & (month <= end)
    dropped = len(df) - int(in_range.sum())
    if dropped:
        logger.info(
            "Filtered %d row(s) outside the configured date range %s..%s "
            "(a landed source file's published coverage can extend beyond "
            "this range even though it was only pulled for a partial overlap)",
            dropped, DATE_RANGE_START, DATE_RANGE_END,
        )
    return df[in_range].copy()

def main() -> None:
    start_time = datetime.datetime.utcnow()

    try:
        source_df, automated_row_count = read_source_files()

        source_df = filter_to_configured_date_range(source_df)

        raw_df = source_df.dropna(how="all").copy()
        dropped_blank = len(source_df) - len(raw_df)

        if MAX_ROWS_TO_INGEST > 0 and len(raw_df) > MAX_ROWS_TO_INGEST:
            logger.info(
                "HDB_MAX_ROWS=%d - testing cap applied, keeping %d of %d rows for raw_iceberg "
                "(every downstream stage inherits this cap too, since they all read from here)",
                MAX_ROWS_TO_INGEST, MAX_ROWS_TO_INGEST, len(raw_df),
            )
            raw_df = raw_df.head(MAX_ROWS_TO_INGEST).copy()

        # NOTE: this used to also write raw_df to raw_iceberg_staging before the
        # DQD check below, then write it AGAIN to raw_iceberg after. Nothing ever
        # reads raw_iceberg_staging (verified repo-wide) - it was paying the full
        # Athena INSERT-batch write cost twice per run for identical rows. Removed
        # 2026-09-12 as part of investigating run duration climbing every run.
        logger.info(
            "%d row(s) ready for raw_iceberg (%d fully-blank row(s) dropped) - "
            "writing after the DQD reconciliation check below",
            len(raw_df), dropped_blank,
        )
        record_audit(
            job_name="job_2_raw_iceberg", stage="raw",
            rows_in=len(source_df), rows_out=len(raw_df), rows_rejected=dropped_blank,
            start_time=start_time,
        )
    except Exception as exc:
        record_stage_result(
            layer="raw_iceberg", table_id=1, status="FAILED",
            my_count=0, reason=str(exc), start_time=start_time,
        )
        send_alert(subject="HDB pipeline - job_2 (raw_iceberg) FAILED", message=str(exc))
        raise

    expected = get_previous_stage_count("ingestion_to_source", table_id=1)
    dqd_status, dqd_reason = reconcile_count(
        my_count=automated_row_count, expected_count=expected,
        layer="raw_iceberg", previous_layer="ingestion_to_source",
    )
    logger.info(dqd_reason)

    if dqd_status == "MISMATCH":
        route_to_failed(raw_df, reason=dqd_reason, stage="raw")
        record_stage_result(
            layer="raw_iceberg", table_id=1, status="MISMATCH",
            my_count=automated_row_count, reason=dqd_reason, start_time=start_time,
        )
        send_alert(subject="HDB pipeline - DQD count check FAILED (raw_iceberg)", message=dqd_reason)
        raise RuntimeError(f"DQD count check failed for raw_iceberg: {dqd_reason}")

    write_by_load_type(raw_df, stage="raw", table_id=1)
    logger.info(
        "job_2 complete: %d rows promoted to raw_iceberg (%d fully-blank rows dropped)",
        len(raw_df), dropped_blank,
    )
    record_stage_result(
        layer="raw_iceberg", table_id=1, status="SUCCEEDED",
        my_count=len(raw_df), reason=dqd_reason, start_time=start_time,
    )

if __name__ == "__main__":
    main()
