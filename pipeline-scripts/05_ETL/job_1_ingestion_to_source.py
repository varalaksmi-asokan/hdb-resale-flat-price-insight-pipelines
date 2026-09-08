
import io
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Tuple

import boto3
import pandas as pd
import requests

from common import get_logger, get_table_parameter, get_watermark, record_audit, send_alert
from context_tracking import record_stage_result
from config import (
    COLLECTION_API_BASE,
    COLLECTION_ID,
    DATASET_API_BASE,
    DATE_RANGE_END,
    DATE_RANGE_START,
    LOOKBACK_WINDOW_DAYS,
    MAX_API_RETRIES,
    MAX_CONCURRENT_DOWNLOADS,
    MAX_DATASETS_TO_INGEST,
    MAX_INGEST_ROWS_PER_DATASET,
    POLL_INTERVAL_SECONDS,
    POLL_TIMEOUT_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    SOURCE_S3_BUCKET,
    SOURCE_S3_PREFIX,
)

logger = get_logger("job_1_ingestion_to_source")
s3_client = boto3.client("s3")

TARGET_TABLE_ID = 1

def _get_with_retry(url: str, **kwargs) -> requests.Response:
    for attempt in range(1, MAX_API_RETRIES + 1):
        resp = requests.get(url, **kwargs)

        if resp.status_code == 429 and attempt < MAX_API_RETRIES:
            retry_after = resp.headers.get("Retry-After")
            wait_seconds = (
                float(retry_after) if retry_after
                else RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1)
            )
            logger.warning(
                "429 rate limited on %s (attempt %d/%d) - waiting %.1fs before retry",
                url, attempt, MAX_API_RETRIES, wait_seconds,
            )
            time.sleep(wait_seconds)
            continue

        resp.raise_for_status()
        return resp

    resp.raise_for_status()
    return resp

@dataclass
class DatasetRef:
    dataset_id: str
    name: str
    coverage_start: date
    coverage_end: date

def _parse_date(iso_ts: str) -> date:
    return datetime.fromisoformat(iso_ts).date()

def get_collection_datasets(collection_id: int) -> List[DatasetRef]:
    url = f"{COLLECTION_API_BASE}/collections/{collection_id}/metadata"
    resp = _get_with_retry(url, params={"withDatasetMetadata": "true"}, timeout=REQUEST_TIMEOUT_SECONDS)
    payload = resp.json()
    if payload.get("errorMsg"):
        raise RuntimeError(f"Collection metadata API error: {payload['errorMsg']}")

    metas = payload["data"].get("datasetMetadata", [])
    if not metas:
        raise RuntimeError("No datasetMetadata returned - did you forget withDatasetMetadata=true?")

    return [
        DatasetRef(
            dataset_id=m["datasetId"],
            name=m.get("name", m["datasetId"]),
            coverage_start=_parse_date(m["coverageStart"]),
            coverage_end=_parse_date(m["coverageEnd"]),
        )
        for m in metas
    ]

def filter_datasets_by_range(datasets: List[DatasetRef], start: date, end: date) -> List[DatasetRef]:
    matching = [d for d in datasets if d.coverage_start <= end and d.coverage_end >= start]
    logger.info("%d of %d datasets overlap %s to %s", len(matching), len(datasets), start, end)
    if not matching:
        raise RuntimeError("No datasets matched the required date range.")
    return matching

def initiate_download(dataset_id: str) -> None:
    url = f"{DATASET_API_BASE}/datasets/{dataset_id}/initiate-download"
    resp = _get_with_retry(url, timeout=REQUEST_TIMEOUT_SECONDS)
    payload = resp.json()
    if payload.get("errorMsg"):
        raise RuntimeError(f"initiate-download error for {dataset_id}: {payload['errorMsg']}")

def poll_download(dataset_id: str) -> str:
    url = f"{DATASET_API_BASE}/datasets/{dataset_id}/poll-download"
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        resp = _get_with_retry(url, timeout=REQUEST_TIMEOUT_SECONDS)
        payload = resp.json()
        if payload.get("errorMsg"):
            raise RuntimeError(f"poll-download error for {dataset_id}: {payload['errorMsg']}")
        download_url = payload.get("data", {}).get("url")
        if download_url:
            return download_url
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Timed out waiting for download URL for dataset_id={dataset_id}")

def _download_csv_with_retry(download_url: str, dataset_id: str) -> pd.DataFrame:
    last_exc = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            with requests.get(download_url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS) as r:
                r.raise_for_status()
                return pd.read_csv(r.raw)
        except (requests.exceptions.RequestException, pd.errors.ParserError) as exc:
            last_exc = exc
            if attempt < MAX_API_RETRIES:
                wait_seconds = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.warning(
                    "File download failed for dataset_id=%s (attempt %d/%d): %s - retrying in %.1fs",
                    dataset_id, attempt, MAX_API_RETRIES, exc, wait_seconds,
                )
                time.sleep(wait_seconds)
            else:
                logger.error(
                    "File download failed for dataset_id=%s after %d attempt(s), giving up: %s",
                    dataset_id, MAX_API_RETRIES, exc,
                )
    raise last_exc

def _sanitize_filename(name: str) -> str:
    return "".join(ch for ch in name if ch.isalnum())

def download_to_source(dataset_id: str, download_url: str, source_name: str) -> Tuple[str, int]:
    filename = _sanitize_filename(source_name) or dataset_id
    s3_key = f"{SOURCE_S3_PREFIX}/dataset_id={dataset_id}/{filename}.csv"

    df = _download_csv_with_retry(download_url, dataset_id)

    total_rows = len(df)
    month = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    start, end = pd.to_datetime(DATE_RANGE_START), pd.to_datetime(DATE_RANGE_END)
    in_range = month.notna() & (month >= start) & (month <= end)
    filtered = df[in_range]
    dropped = total_rows - len(filtered)

    if MAX_INGEST_ROWS_PER_DATASET > 0 and len(filtered) > MAX_INGEST_ROWS_PER_DATASET:
        logger.info(
            "MAX_INGEST_ROWS_PER_DATASET=%d - truncating %s from %d in-range row(s) for this test run",
            MAX_INGEST_ROWS_PER_DATASET, dataset_id, len(filtered),
        )
        filtered = filtered.head(MAX_INGEST_ROWS_PER_DATASET)

    csv_bytes = filtered.to_csv(index=False).encode("utf-8")
    s3_client.upload_fileobj(io.BytesIO(csv_bytes), SOURCE_S3_BUCKET, s3_key)

    row_count = len(filtered)
    logger.info(
        "Landed source file: s3://%s/%s (%d row(s) in range %s..%s; %d row(s) outside range dropped before upload, out of %d total in the published file)",
        SOURCE_S3_BUCKET, s3_key, row_count, DATE_RANGE_START, DATE_RANGE_END, dropped, total_rows,
    )
    return f"s3://{SOURCE_S3_BUCKET}/{s3_key}", row_count

def resolve_effective_date_range() -> Tuple[date, date]:
    full_start = date.fromisoformat(DATE_RANGE_START)
    full_end = date.fromisoformat(DATE_RANGE_END)

    load_type = get_table_parameter(TARGET_TABLE_ID, "load_type", default="FULL").strip().upper()
    if load_type == "FULL":
        logger.info("load_type=FULL -> pulling full configured range %s to %s", full_start, full_end)
        return full_start, full_end

    watermark_raw = get_watermark(TARGET_TABLE_ID, default=None)
    if not watermark_raw or watermark_raw.strip().startswith("1900-01-01"):
        logger.info(
            "load_type=%s but no watermark set yet - first incremental run, pulling full configured range %s to %s",
            load_type, full_start, full_end,
        )
        return full_start, full_end

    lookback_days = int(get_table_parameter(TARGET_TABLE_ID, "lookback_window", default=str(LOOKBACK_WINDOW_DAYS)))
    watermark_date = datetime.fromisoformat(watermark_raw.strip()).date()
    effective_start = max(full_start, watermark_date - timedelta(days=lookback_days))

    logger.info(
        "load_type=%s -> narrowing pull to %s to %s (watermark=%s, lookback_window=%dd)",
        load_type, effective_start, full_end, watermark_date, lookback_days,
    )
    return effective_start, full_end

def _ingest_one_dataset(d: "DatasetRef") -> str:
    initiate_download(d.dataset_id)
    url = poll_download(d.dataset_id)
    return download_to_source(d.dataset_id, url, d.name)

def _ingest_datasets_in_batches(datasets: List["DatasetRef"], max_concurrent: int) -> List[str]:
    results: List[Tuple[str, int]] = []
    with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
        futures = {pool.submit(_ingest_one_dataset, d): d for d in datasets}
        for future in as_completed(futures):
            d = futures[future]
            result = future.result()
            logger.info("Batch ingestion progress: %d/%d datasets landed (just finished %s)",
                        len(results) + 1, len(datasets), d.dataset_id)
            results.append(result)
    return results

def main() -> List[str]:
    start_time = datetime.utcnow()

    try:
        datasets = get_collection_datasets(COLLECTION_ID)
        range_start, range_end = resolve_effective_date_range()
        matching = filter_datasets_by_range(datasets, range_start, range_end)

        if MAX_DATASETS_TO_INGEST > 0 and len(matching) > MAX_DATASETS_TO_INGEST:
            logger.info(
                "HDB_MAX_DATASETS=%d - testing cap applied, ingesting %d of %d matched datasets "
                "(dropped: %s)",
                MAX_DATASETS_TO_INGEST, MAX_DATASETS_TO_INGEST, len(matching),
                [d.dataset_id for d in matching[MAX_DATASETS_TO_INGEST:]],
            )
            matching = matching[:MAX_DATASETS_TO_INGEST]

        results = _ingest_datasets_in_batches(matching, MAX_CONCURRENT_DOWNLOADS)
        source_paths = [uri for uri, _rows in results]
        total_rows = sum(rows for _uri, rows in results)

        logger.info("job_1 complete: %d source files landed, %d total data row(s)", len(source_paths), total_rows)
        record_audit(
            job_name="job_1_ingestion_to_source", stage="source",
            rows_in=len(matching), rows_out=total_rows, rows_rejected=len(matching) - len(source_paths),
            start_time=start_time,
        )
    except Exception as exc:
        record_stage_result(
            layer="ingestion_to_source", table_id=TARGET_TABLE_ID, status="FAILED",
            my_count=0, reason=str(exc), start_time=start_time,
        )
        send_alert(subject="HDB pipeline - job_1 (ingestion_to_source) FAILED", message=str(exc))
        raise

    record_stage_result(
        layer="ingestion_to_source", table_id=TARGET_TABLE_ID, status="SUCCEEDED",
        my_count=total_rows,
        reason=(
            f"landed {total_rows} row(s) across {len(source_paths)} file(s) - "
            f"source_location=data.gov.sg (collection {COLLECTION_ID}), "
            f"target_location=s3://{SOURCE_S3_BUCKET}/{SOURCE_S3_PREFIX}/"
        ),
        start_time=start_time,
    )

    return source_paths

if __name__ == "__main__":
    main()
