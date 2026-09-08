
import datetime
import hashlib

from common import get_logger, merge_iceberg, read_iceberg, record_audit, route_to_failed, send_alert
from context_tracking import get_previous_stage_count, reconcile_count, record_stage_result

logger = get_logger("job_5_hashed_iceberg")

def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def main() -> None:
    start_time = datetime.datetime.utcnow()

    try:
        transformed_df = read_iceberg("transformed")
        logger.info("Read %d rows from transformed_iceberg", len(transformed_df))

        missing_identifier = transformed_df["resale_identifier"].isna() | (transformed_df["resale_identifier"] == "")
        hashable = transformed_df[~missing_identifier].copy()
        unhashable = transformed_df[missing_identifier]
        route_to_failed(unhashable, reason="missing_resale_identifier", stage="hashed")

        hashable["resale_identifier_hash"] = hashable["resale_identifier"].apply(hash_value)

        hashable = hashable.drop(columns=["resale_identifier"], errors="ignore")

        merge_iceberg(hashable, stage="hashed_staging")
        logger.info(
            "staged %d row(s) into hashed_iceberg_staging, %d rejected (missing identifier)",
            len(hashable), len(unhashable),
        )
        record_audit(
            job_name="job_5_hashed_iceberg", stage="hashed",
            rows_in=len(transformed_df), rows_out=len(hashable), rows_rejected=len(unhashable),
            start_time=start_time,
        )
    except Exception as exc:
        record_stage_result(
            layer="hashed_iceberg", table_id=1, status="FAILED",
            my_count=0, reason=str(exc), start_time=start_time,
        )
        send_alert(subject="HDB pipeline - job_5 (hashed_iceberg) FAILED", message=str(exc))
        raise

    expected = get_previous_stage_count("transformed_iceberg", table_id=1)
    dqd_status, dqd_reason = reconcile_count(
        my_count=len(hashable), expected_count=expected,
        layer="hashed_iceberg", previous_layer="transformed_iceberg",
        my_rejected_count=len(unhashable),
    )
    logger.info(dqd_reason)

    if dqd_status == "MISMATCH":
        route_to_failed(hashable, reason=dqd_reason, stage="hashed")
        record_stage_result(
            layer="hashed_iceberg", table_id=1, status="MISMATCH",
            my_count=len(hashable), reason=dqd_reason, start_time=start_time,
        )
        send_alert(subject="HDB pipeline - DQD count check FAILED (hashed_iceberg)", message=dqd_reason)
        raise RuntimeError(f"DQD count check failed for hashed_iceberg: {dqd_reason}")

    merge_iceberg(hashable, stage="hashed")
    logger.info(
        "job_5 complete: %d row(s) upserted into hashed_iceberg, %d rejected (missing identifier)",
        len(hashable), len(unhashable),
    )
    record_stage_result(
        layer="hashed_iceberg", table_id=1, status="SUCCEEDED",
        my_count=len(hashable), reason=dqd_reason, start_time=start_time,
    )

if __name__ == "__main__":
    main()
