
import datetime
import re
from datetime import date

import pandas as pd

from common import compute_surrogate_key, get_logger, read_iceberg, record_audit, route_to_failed, send_alert, write_by_load_type
from config import LEASE_YEARS, MIN_CATEGORY_FREQUENCY
from context_tracking import get_previous_stage_count, reconcile_count, record_stage_result

logger = get_logger("job_3_cleaned_iceberg")

STOREY_RANGE_PATTERN = re.compile(r"^\d{2} TO \d{2}$")

FLOOR_AREA_MIN_SQM = 20
FLOOR_AREA_MAX_SQM = 300

NORMALIZE_TEXT_COLUMNS = ["town", "street_name", "flat_model"]

def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in NORMALIZE_TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
    return df

def validate_date(df: pd.DataFrame) -> pd.Series:
    parsed = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    return parsed.notna()

def validate_categorical(df: pd.DataFrame, column: str) -> pd.Series:
    values = df[column].astype(str).str.strip()
    non_blank = (values != "") & df[column].notna()
    frequency = values.map(values[non_blank].value_counts())
    return non_blank & (frequency >= MIN_CATEGORY_FREQUENCY)

def validate_storey_range(df: pd.DataFrame) -> pd.Series:
    return df["storey_range"].astype(str).str.strip().str.match(STOREY_RANGE_PATTERN)

def validate_floor_area_bounds(df: pd.DataFrame) -> pd.Series:
    area = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    return area.notna() & (area >= FLOOR_AREA_MIN_SQM) & (area <= FLOOR_AREA_MAX_SQM)

def validate_lease_commence_vs_transaction(df: pd.DataFrame) -> pd.Series:
    def _valid(lease_year, month_str) -> bool:
        try:
            return int(lease_year) <= int(str(month_str).split("-")[0])
        except (TypeError, ValueError, IndexError):
            return False
    return pd.Series(
        [_valid(ly, m) for ly, m in zip(df["lease_commence_date"], df["month"])], index=df.index
    )

def validate_resale_price_positive(df: pd.DataFrame) -> pd.Series:
    price = pd.to_numeric(df["resale_price"], errors="coerce")
    return price.notna() & (price > 0)

def run_field_validations(df: pd.DataFrame) -> pd.DataFrame:
    checks = {
        "invalid_date": ~validate_date(df),
        "invalid_town": ~validate_categorical(df, "town"),
        "invalid_flat_type": ~validate_categorical(df, "flat_type"),
        "invalid_flat_model": ~validate_categorical(df, "flat_model"),
        "invalid_storey_range": ~validate_storey_range(df),
        "invalid_floor_area": ~validate_floor_area_bounds(df),
        "lease_commence_after_transaction": ~validate_lease_commence_vs_transaction(df),
        "invalid_resale_price": ~validate_resale_price_positive(df),
    }
    errors = pd.Series([[] for _ in range(len(df))], index=df.index)
    for rule_name, failed_mask in checks.items():
        errors.loc[failed_mask] = errors.loc[failed_mask].apply(lambda lst, r=rule_name: lst + [r])
    df = df.copy()
    df["_validation_errors"] = errors
    return df

def recompute_remaining_lease(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    today = date.today()

    def _remaining(lease_commence_year) -> pd.Series:
        try:
            commence_year = int(lease_commence_year)
        except (TypeError, ValueError):
            return pd.Series({"remaining_lease_years": None, "remaining_lease_months": None})

        elapsed_months = (today.year - commence_year) * 12 + (today.month - 1)
        if today.day > 1:
            elapsed_months += 1
        remaining_months_total = LEASE_YEARS * 12 - elapsed_months
        remaining_months_total = max(remaining_months_total, 0)
        return pd.Series({
            "remaining_lease_years": remaining_months_total // 12,
            "remaining_lease_months": remaining_months_total % 12,
        })

    lease_cols = df["lease_commence_date"].apply(_remaining)
    df["remaining_lease_years"] = lease_cols["remaining_lease_years"]
    df["remaining_lease_months"] = lease_cols["remaining_lease_months"]
    return df

def resolve_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    key_cols = [c for c in df.columns if c not in ("resale_price", "surrogate_key") and not c.startswith("_")]
    df = df.sort_values("resale_price", ascending=False)
    is_dup = df.duplicated(subset=key_cols, keep="first")
    kept, discarded = df[~is_dup], df[is_dup]
    logger.info("Duplicate-key resolution: kept %d, discarded %d (lower price)", len(kept), len(discarded))

    if kept.duplicated(subset=key_cols).any():
        raise RuntimeError(
            "resolve_duplicates() invariant violated: duplicate composite "
            "key(s) remain in kept rows after dedup"
        )
    return kept, discarded

def flag_anomalous_price(df: pd.DataFrame) -> pd.Series:
    def _iqr_outlier_mask(group: pd.Series) -> pd.Series:
        q1, q3 = group.quantile(0.25), group.quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        return (group < lower) | (group > upper)

    return df.groupby(["town", "flat_type"])["resale_price"].transform(_iqr_outlier_mask)

def main() -> None:
    start_time = datetime.datetime.utcnow()

    try:
        raw_df = read_iceberg("raw")
        logger.info("Read %d rows from raw_iceberg", len(raw_df))

        raw_df["surrogate_key"] = compute_surrogate_key(raw_df)

        raw_df = normalize_text_columns(raw_df)

        validated = run_field_validations(raw_df)
        field_failed_mask = validated["_validation_errors"].apply(len).gt(0)
        field_passed = validated[~field_failed_mask].drop(columns=["_validation_errors"])
        field_failed = validated[field_failed_mask].copy()
        field_failed["_validation_errors"] = field_failed["_validation_errors"].apply(", ".join)

        route_to_failed(field_failed, reason="field_validation_failed", stage="cleaned")

        kept, discarded_dupes = resolve_duplicates(field_passed)
        route_to_failed(discarded_dupes, reason="duplicate_key_lower_price", stage="cleaned")

        kept = recompute_remaining_lease(kept)

        anomaly_mask = flag_anomalous_price(kept)
        clean_final = kept[~anomaly_mask]
        anomalous = kept[anomaly_mask]
        route_to_failed(anomalous, reason="anomalous_price_iqr_outlier", stage="cleaned")

        total_rejected = len(field_failed) + len(discarded_dupes) + len(anomalous)

        # NOTE: this used to also write clean_final to cleaned_iceberg_staging before
        # the DQD check below, then write it AGAIN to cleaned_iceberg after. Nothing
        # ever reads cleaned_iceberg_staging (verified repo-wide) - it was paying the
        # full Athena INSERT-batch write cost twice per run for identical rows.
        # Removed 2026-09-12 as part of investigating run duration climbing every run.
        logger.info(
            "%d row(s) ready for cleaned_iceberg (%d field-rejects, %d dupes, %d anomalies) - "
            "writing after the DQD reconciliation check below",
            len(clean_final), len(field_failed), len(discarded_dupes), len(anomalous),
        )
        record_audit(
            job_name="job_3_cleaned_iceberg", stage="cleaned",
            rows_in=len(raw_df), rows_out=len(clean_final), rows_rejected=total_rejected,
            start_time=start_time,
        )
    except Exception as exc:
        record_stage_result(
            layer="cleaned_iceberg", table_id=1, status="FAILED",
            my_count=0, reason=str(exc), start_time=start_time,
        )
        send_alert(subject="HDB pipeline - job_3 (cleaned_iceberg) FAILED", message=str(exc))
        raise

    expected = get_previous_stage_count("raw_iceberg", table_id=1)
    dqd_status, dqd_reason = reconcile_count(
        my_count=len(clean_final), expected_count=expected,
        layer="cleaned_iceberg", previous_layer="raw_iceberg",
        my_rejected_count=total_rejected,
    )
    logger.info(dqd_reason)

    if dqd_status == "MISMATCH":
        route_to_failed(clean_final, reason=dqd_reason, stage="cleaned")
        record_stage_result(
            layer="cleaned_iceberg", table_id=1, status="MISMATCH",
            my_count=len(clean_final), reason=dqd_reason, start_time=start_time,
        )
        send_alert(subject="HDB pipeline - DQD count check FAILED (cleaned_iceberg)", message=dqd_reason)
        raise RuntimeError(f"DQD count check failed for cleaned_iceberg: {dqd_reason}")

    write_by_load_type(clean_final, stage="cleaned", table_id=1)
    logger.info(
        "job_3 complete: %d rows promoted to cleaned_iceberg, %d rejected",
        len(clean_final), total_rejected,
    )
    record_stage_result(
        layer="cleaned_iceberg", table_id=1, status="SUCCEEDED",
        my_count=len(clean_final), reason=dqd_reason, start_time=start_time,
    )

if __name__ == "__main__":
    main()
