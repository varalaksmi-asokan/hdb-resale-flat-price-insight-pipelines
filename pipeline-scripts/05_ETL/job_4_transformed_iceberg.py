
import datetime

import pandas as pd

from common import get_logger, read_iceberg, record_audit, route_to_failed, send_alert, write_by_load_type
from context_tracking import get_previous_stage_count, reconcile_count, record_stage_result

logger = get_logger("job_4_transformed_iceberg")

def _block_digits(block) -> str:
    digits = "".join(ch for ch in str(block) if ch.isdigit())
    return digits[:3].zfill(3) if digits else None

def _avg_price_digits(avg_price: float) -> str:
    if pd.isna(avg_price):
        return None
    int_part = str(int(avg_price))
    return int_part[:2].zfill(2)

def _month_digits(month_str: str) -> str:
    try:
        return str(month_str).split("-")[1].zfill(2)
    except (IndexError, AttributeError):
        return None

def _town_char(town) -> str:
    town = str(town).strip()
    return town[0].upper() if town else None

def build_resale_identifier(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    group_avg = df.groupby(["month", "town", "flat_type"])["resale_price"].transform("mean")

    block_part = df["block"].apply(_block_digits)
    price_part = group_avg.apply(_avg_price_digits)
    month_part = df["month"].apply(_month_digits)
    town_part = df["town"].apply(_town_char)

    identifier = (
        "S"
        + block_part.fillna("")
        + price_part.fillna("")
        + month_part.fillna("")
        + town_part.fillna("")
    )
    has_all_components = (
        block_part.notna() & price_part.notna() & month_part.notna() & town_part.notna()
    )
    df["resale_identifier"] = identifier
    df["_identifier_valid"] = has_all_components & (identifier.str.len() == 9)
    return df

def resolve_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    key_cols = ["resale_identifier"]
    df = df.sort_values("resale_price", ascending=False)
    is_dup = df.duplicated(subset=key_cols, keep="first")
    kept, discarded = df[~is_dup], df[is_dup]
    logger.info(
        "Resale Identifier duplicate resolution: kept %d, discarded %d (lower price)",
        len(kept), len(discarded),
    )

    if kept.duplicated(subset=key_cols).any():
        raise RuntimeError(
            "resolve_duplicates() invariant violated: duplicate "
            "resale_identifier value(s) remain in kept rows after dedup"
        )
    return kept, discarded

def main() -> None:
    start_time = datetime.datetime.utcnow()

    try:
        cleaned_df = read_iceberg("cleaned")
        logger.info("Read %d rows from cleaned_iceberg", len(cleaned_df))

        with_identifier = build_resale_identifier(cleaned_df)

        valid = with_identifier[with_identifier["_identifier_valid"]].drop(columns=["_identifier_valid"])
        invalid = with_identifier[~with_identifier["_identifier_valid"]]
        route_to_failed(invalid, reason="incomplete_resale_identifier", stage="transformed")

        kept, discarded_dupes = resolve_duplicates(valid)
        route_to_failed(discarded_dupes, reason="duplicate_key_lower_price", stage="transformed")

        total_rejected = len(invalid) + len(discarded_dupes)

        write_by_load_type(kept, stage="transformed_staging", table_id=1)
        logger.info(
            "staged %d row(s) to transformed_iceberg_staging (%d identifier-rejects, %d dupes)",
            len(kept), len(invalid), len(discarded_dupes),
        )
        record_audit(
            job_name="job_4_transformed_iceberg", stage="transformed",
            rows_in=len(cleaned_df), rows_out=len(kept), rows_rejected=total_rejected,
            start_time=start_time,
        )
    except Exception as exc:
        record_stage_result(
            layer="transformed_iceberg", table_id=1, status="FAILED",
            my_count=0, reason=str(exc), start_time=start_time,
        )
        send_alert(subject="HDB pipeline - job_4 (transformed_iceberg) FAILED", message=str(exc))
        raise

    expected = get_previous_stage_count("cleaned_iceberg", table_id=1)
    dqd_status, dqd_reason = reconcile_count(
        my_count=len(kept), expected_count=expected,
        layer="transformed_iceberg", previous_layer="cleaned_iceberg",
        my_rejected_count=total_rejected,
    )
    logger.info(dqd_reason)

    if dqd_status == "MISMATCH":
        route_to_failed(kept, reason=dqd_reason, stage="transformed")
        record_stage_result(
            layer="transformed_iceberg", table_id=1, status="MISMATCH",
            my_count=len(kept), reason=dqd_reason, start_time=start_time,
        )
        send_alert(subject="HDB pipeline - DQD count check FAILED (transformed_iceberg)", message=dqd_reason)
        raise RuntimeError(f"DQD count check failed for transformed_iceberg: {dqd_reason}")

    write_by_load_type(kept, stage="transformed", table_id=1)
    logger.info(
        "job_4 complete: %d rows promoted to transformed_iceberg, %d rejected",
        len(kept), total_rejected,
    )
    record_stage_result(
        layer="transformed_iceberg", table_id=1, status="SUCCEEDED",
        my_count=len(kept), reason=dqd_reason, start_time=start_time,
    )

if __name__ == "__main__":
    main()
