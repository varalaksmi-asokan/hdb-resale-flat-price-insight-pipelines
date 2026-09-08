
import datetime as dt
import json
from datetime import datetime, timezone

import pandas as pd

from common import get_logger, read_iceberg, record_audit, send_alert
from config import AUDIT_S3_BUCKET, MIN_CATEGORY_FREQUENCY
from context_tracking import record_stage_result

logger = get_logger("job_2b_data_profiling")

REPORT_S3_PREFIX = f"s3://{AUDIT_S3_BUCKET}/profiling_reports"

def _column_profile(series: pd.Series) -> dict:
    non_null = series.dropna()
    profile = {
        "dtype": str(series.dtype),
        "row_count": int(len(series)),
        "null_count": int(series.isna().sum()),
        "null_pct": round(float(series.isna().mean()) * 100, 2),
        "distinct_count": int(non_null.nunique()),
    }

    if pd.api.types.is_numeric_dtype(series):
        profile.update({
            "min": float(non_null.min()) if not non_null.empty else None,
            "max": float(non_null.max()) if not non_null.empty else None,
            "mean": float(non_null.mean()) if not non_null.empty else None,
            "std": float(non_null.std()) if not non_null.empty else None,
            "q1": float(non_null.quantile(0.25)) if not non_null.empty else None,
            "median": float(non_null.quantile(0.5)) if not non_null.empty else None,
            "q3": float(non_null.quantile(0.75)) if not non_null.empty else None,
        })
    else:
        top_values = non_null.astype(str).value_counts().head(20)
        profile["top_values"] = top_values.to_dict()
        profile["distinct_values_sample"] = sorted(non_null.astype(str).unique().tolist())[:50]

    return profile

def _storey_range_profile(df: pd.DataFrame) -> dict:
    if "storey_range" not in df.columns:
        return {}
    parts = df["storey_range"].astype(str).str.strip().str.extract(r"^(\d{2}) TO (\d{2})$")
    low = pd.to_numeric(parts[0], errors="coerce")
    high = pd.to_numeric(parts[1], errors="coerce")
    valid_pair = low.notna() & high.notna()
    anomalous = valid_pair & ((low > high) | ((low == 0) & (high == 0)))
    return {
        "min_storey": float(low[valid_pair].min()) if valid_pair.any() else None,
        "max_storey": float(high[valid_pair].max()) if valid_pair.any() else None,
        "unparseable_rows": int((~valid_pair).sum()),
        "anomalous_range_rows": int(anomalous.sum()),
    }

def _block_format_profile(df: pd.DataFrame) -> dict:
    if "block" not in df.columns:
        return {}
    values = df["block"].astype(str).str.strip()
    is_numeric = values.str.match(r"^\d+$")
    is_alnum = values.str.match(r"^\d+[A-Za-z]+$")
    other = ~(is_numeric | is_alnum)
    return {
        "numeric_count": int(is_numeric.sum()),
        "alphanumeric_count": int(is_alnum.sum()),
        "other_format_count": int(other.sum()),
        "other_format_sample": sorted(values[other].unique().tolist())[:20],
    }

def _rare_category_profile(df: pd.DataFrame, columns: list) -> dict:
    rare = {}
    for col in columns:
        if col not in df.columns:
            continue
        values = df[col].astype(str).str.strip()
        counts = values[values != ""].value_counts()
        rare[col] = counts[counts < MIN_CATEGORY_FREQUENCY].to_dict()
    return rare

def profile_dataframe(df: pd.DataFrame, dataset_name: str) -> dict:
    report = {
        "dataset_name": dataset_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": {col: _column_profile(df[col]) for col in df.columns},
    }

    if {"resale_price"}.issubset(df.columns):
        key_cols = [c for c in df.columns if c not in ("resale_price", "surrogate_key")]
        dup_mask = df.duplicated(subset=key_cols, keep=False)
        report["composite_key_duplicate_rows"] = int(dup_mask.sum())

    report["storey_range_profile"] = _storey_range_profile(df)
    report["block_format_profile"] = _block_format_profile(df)
    report["rare_categories"] = _rare_category_profile(df, ["town", "flat_type", "flat_model"])

    return report

def render_markdown_summary(report: dict) -> str:
    lines = [
        f"# Data Profiling Report - {report['dataset_name']}",
        f"Generated: {report['generated_at']}",
        "",
        f"- Rows: {report['row_count']}",
        f"- Columns: {report['column_count']}",
    ]
    if "composite_key_duplicate_rows" in report:
        lines.append(f"- Rows sharing a composite key (differ only by resale_price): "
                      f"{report['composite_key_duplicate_rows']}")
    sp = report.get("storey_range_profile") or {}
    if sp:
        lines.append(
            f"- Storey range: min={sp.get('min_storey')}, max={sp.get('max_storey')}, "
            f"unparseable={sp.get('unparseable_rows')}, "
            f"anomalous (low>high or 00 TO 00)={sp.get('anomalous_range_rows')}"
        )
    bp = report.get("block_format_profile") or {}
    if bp:
        lines.append(
            f"- Block formats: numeric={bp.get('numeric_count')}, "
            f"alphanumeric={bp.get('alphanumeric_count')}, other={bp.get('other_format_count')} "
            f"(sample: {bp.get('other_format_sample')})"
        )
    rare = report.get("rare_categories") or {}
    if any(rare.values()):
        lines.append("- Rare categories (< MIN_CATEGORY_FREQUENCY occurrences - possible typos):")
        for col, vals in rare.items():
            if vals:
                lines.append(f"  - {col}: {vals}")
    lines.append("")
    lines.append("## Per-column summary")
    for col, prof in report["columns"].items():
        lines.append(f"\n### `{col}` ({prof['dtype']})")
        lines.append(f"- nulls: {prof['null_count']} ({prof['null_pct']}%) | distinct: {prof['distinct_count']}")
        if "mean" in prof:
            lines.append(
                f"- min={prof['min']}, q1={prof['q1']}, median={prof['median']}, "
                f"q3={prof['q3']}, max={prof['max']}, mean={round(prof['mean'], 2) if prof['mean'] is not None else None}, "
                f"std={round(prof['std'], 2) if prof['std'] is not None else None}"
            )
        else:
            top = list(prof.get("top_values", {}).items())[:5]
            lines.append(f"- top values: {top}")
    return "\n".join(lines)

def main() -> None:
    start_time = dt.datetime.utcnow()

    try:
        raw_df = read_iceberg("raw")
        logger.info("Read %d rows from raw_iceberg for profiling", len(raw_df))

        report = profile_dataframe(raw_df, dataset_name="raw_iceberg")
        markdown = render_markdown_summary(report)

        run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = f"{REPORT_S3_PREFIX}/raw_iceberg_{run_stamp}.json"
        md_path = f"{REPORT_S3_PREFIX}/raw_iceberg_{run_stamp}.md"

        _write_text_to_s3(json.dumps(report, indent=2, default=str), json_path)
        _write_text_to_s3(markdown, md_path)

        logger.info("job_2b complete: profiling report written to %s and %s", json_path, md_path)
        record_audit(
            job_name="job_2b_data_profiling", stage="raw",
            rows_in=len(raw_df), rows_out=len(raw_df), rows_rejected=0,
            start_time=start_time,
        )
    except Exception as exc:
        record_stage_result(
            layer="profiling", table_id=1, status="FAILED",
            my_count=0, reason=str(exc), start_time=start_time,
        )
        send_alert(subject="HDB pipeline - job_2b (data_profiling) FAILED", message=str(exc))
        raise

    record_stage_result(
        layer="profiling", table_id=1, status="SUCCEEDED",
        my_count=len(raw_df),
        reason=f"profiled {len(raw_df)} row(s) from raw_iceberg, report written to {json_path}",
        start_time=start_time,
    )

def _write_text_to_s3(text: str, s3_path: str) -> None:
    import boto3
    from urllib.parse import urlparse

    parsed = urlparse(s3_path)
    boto3.client("s3").put_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=text.encode("utf-8"))

if __name__ == "__main__":
    main()
