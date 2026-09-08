
import hashlib
import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
import pandas as pd

from config import (
    ATHENA_WORKGROUP,
    AUDIT_S3_BUCKET,
    AWS_REGION,
    COLUMN_TYPES,
    GLUE_DATABASE,
    MAX_CONCURRENT_ATHENA_INSERTS,
    MAX_CONCURRENT_S3_READS,
    NATURAL_KEY_COLUMNS,
    SNS_TOPIC_ARN_OVERRIDE,
    SNS_TOPIC_NAME,
    TABLES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

for _noisy_logger in ("boto3", "botocore", "urllib3", "s3transfer", "awswrangler"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

_BOTO3_SESSION = boto3.Session(region_name=AWS_REGION)

ATHENA_RESULTS_LOCATION = f"s3://{AUDIT_S3_BUCKET}/athena-results/"

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

_account_id_cache = None

def get_account_id() -> str:
    global _account_id_cache
    if _account_id_cache is None:
        _account_id_cache = _BOTO3_SESSION.client("sts").get_caller_identity()["Account"]
    return _account_id_cache

def _iceberg_table_exists(table: str) -> bool:
    glue = _BOTO3_SESSION.client("glue")
    try:
        glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

ICEBERG_PROPAGATION_RETRIES = 3
ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS = 8

def _athena_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "STRING"

def _canonical_athena_type(col: str, series: pd.Series, existing_types: dict = None) -> str:
    canonical = COLUMN_TYPES.get(col.lower())
    if canonical:
        return canonical
    if existing_types:
        existing = existing_types.get(col.lower())
        if existing:
            return existing
    return _athena_type_for_series(series)

def _sql_literal(value, athena_type: str) -> str:
    if not pd.api.types.is_scalar(value):
        is_missing = False
    else:
        try:
            is_missing = value is None or pd.isna(value)
        except (TypeError, ValueError):
            is_missing = False
    if is_missing:

        return f"CAST(NULL AS {athena_type})"

    if athena_type == "BOOLEAN":
        return "true" if value else "false"
    if athena_type == "TIMESTAMP":
        ts = pd.Timestamp(value)
        return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
    if athena_type in ("BIGINT", "DOUBLE"):

        return f"CAST({value} AS {athena_type})"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"

def _create_iceberg_table_sql(table: str, location: str, df: pd.DataFrame) -> str:
    col_types = {c: _canonical_athena_type(c, df[c]) for c in df.columns}
    cols_sql = ",\n    ".join(f'{c} {t}' for c, t in col_types.items())
    return f"""
    CREATE TABLE IF NOT EXISTS {GLUE_DATABASE}.{table} (
        {cols_sql}
    )
    LOCATION '{location}'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet',
        'write_compression' = 'snappy'
    )
    """

ICEBERG_INSERT_MAX_PAYLOAD_BYTES = 230_000

_ICEBERG_CONFLICT_PATTERNS = ("conflict", "concurrent", "commit")

def _execute_athena_insert_batch(sql: str, description: str, max_retries: int = 4) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            execute_athena_sql(sql, description)
            return
        except RuntimeError as exc:
            is_conflict = any(p in str(exc).lower() for p in _ICEBERG_CONFLICT_PATTERNS)
            if not is_conflict or attempt == max_retries:
                raise
            wait_seconds = 1.5 * (2 ** (attempt - 1)) + random.uniform(0, 1)
            get_logger("athena_sql").warning(
                "Possible concurrent-commit conflict on %s (attempt %d/%d) - retrying in %.1fs: %s",
                description, attempt, max_retries, wait_seconds, exc,
            )
            time.sleep(wait_seconds)

def _insert_iceberg_rows(table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

    existing_types = _existing_iceberg_column_types(table) or {}
    col_types = {
        c: _canonical_athena_type(c, df[c], existing_types)
        for c in df.columns
    }
    columns = list(df.columns)
    cols_sql = ", ".join(columns)
    prefix = f"INSERT INTO {GLUE_DATABASE}.{table} ({cols_sql}) VALUES\n"
    max_values_bytes = ICEBERG_INSERT_MAX_PAYLOAD_BYTES - len(prefix.encode("utf-8"))

    row_strings = [
        "(" + ", ".join(_sql_literal(val, col_types[col]) for col, val in zip(columns, row)) + ")"
        for row in df.itertuples(index=False, name=None)
    ]

    batches = []
    current, current_bytes = [], 0
    for row_str in row_strings:
        row_bytes = len(row_str.encode("utf-8")) + 2
        if current and current_bytes + row_bytes > max_values_bytes:
            batches.append(current)
            current, current_bytes = [], 0
        current.append(row_str)
        current_bytes += row_bytes
    if current:
        batches.append(current)

    n_batches = len(batches)

    def _run_batch(i: int, batch_rows: list) -> None:
        values_sql = ",\n".join(batch_rows)
        sql = f"{prefix}{values_sql}"
        _execute_athena_insert_batch(
            sql, f"INSERT batch {i + 1}/{n_batches} into {table} ({len(batch_rows)} row(s))"
        )

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ATHENA_INSERTS) as pool:
        futures = [pool.submit(_run_batch, i, batch) for i, batch in enumerate(batches)]
        for future in futures:
            future.result()

def _existing_iceberg_columns(table: str):
    glue = _BOTO3_SESSION.client("glue")
    try:
        tbl = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None
    return {c["Name"].lower() for c in tbl["StorageDescriptor"]["Columns"]}

_HIVE_TYPE_TO_ATHENA_TYPE = {
    "string": "STRING",
    "varchar": "STRING",
    "bigint": "BIGINT",
    "int": "BIGINT",
    "integer": "BIGINT",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}

def _existing_iceberg_column_types(table: str):
    glue = _BOTO3_SESSION.client("glue")
    try:
        tbl = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None
    types = {}
    for c in tbl["StorageDescriptor"]["Columns"]:
        raw_type = c["Type"].lower().split("(")[0].strip()
        types[c["Name"].lower()] = _HIVE_TYPE_TO_ATHENA_TYPE.get(raw_type, "STRING")
    return types

def _ensure_iceberg_columns(table: str, df: pd.DataFrame) -> None:
    existing = _existing_iceberg_columns(table)
    if existing is None:
        return
    col_types = {c: _canonical_athena_type(c, df[c]) for c in df.columns}
    missing = [(c, t) for c, t in col_types.items() if c.lower() not in existing]
    if not missing:
        return
    cols_sql = ", ".join(f"{c} {t}" for c, t in missing)
    execute_athena_sql(
        f"ALTER TABLE {GLUE_DATABASE}.{table} ADD COLUMNS ({cols_sql})",
        f"ALTER TABLE {table} ADD COLUMNS ({cols_sql})",
    )

def _write_df_iceberg_raw(df: pd.DataFrame, table: str, location: str, mode: str) -> None:
    execute_athena_sql(_create_iceberg_table_sql(table, location, df), f"CREATE TABLE IF NOT EXISTS {table}")
    _ensure_iceberg_columns(table, df)
    _insert_iceberg_rows(table, df)

def _to_iceberg_with_retry(df: pd.DataFrame, table: str, location: str, mode: str) -> None:
    logger = get_logger("to_iceberg_retry")
    expected_new_rows = len(df) if df is not None else None

    before_count = None
    if mode in ("append", "overwrite") and table and expected_new_rows and _iceberg_table_exists(table):
        before_count = _iceberg_row_count(table)

    for attempt in range(1, ICEBERG_PROPAGATION_RETRIES + 1):
        try:
            _write_df_iceberg_raw(df, table, location, mode)
            return
        except RuntimeError as exc:
            if "cannot find the requested entity" not in str(exc).lower():
                raise

            if expected_new_rows and _iceberg_table_exists(table):
                after_count = _iceberg_row_count(table)
                write_already_landed = (
                    (mode == "append" and before_count is not None and after_count >= before_count + expected_new_rows)
                    or (mode == "overwrite" and after_count == expected_new_rows)
                )
                if write_already_landed:
                    logger.warning(
                        "%s already reflects this write (mode=%s, %s -> %d rows) despite the propagation-lag "
                        "error below - NOT retrying, to avoid duplicating the data: %s",
                        table, mode, before_count, after_count, exc,
                    )
                    return

            if attempt == ICEBERG_PROPAGATION_RETRIES:
                raise
            logger.warning(
                "Iceberg catalog propagation lag (attempt %d/%d) - retrying in %ds: %s",
                attempt, ICEBERG_PROPAGATION_RETRIES, ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS, exc,
            )
            time.sleep(ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS)

def compute_surrogate_key(df: pd.DataFrame, key_columns: list = None) -> pd.Series:
    key_columns = key_columns or [c for c in NATURAL_KEY_COLUMNS if c in df.columns]
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        raise KeyError(f"Cannot compute surrogate key - missing columns: {missing}")
    concat = df[key_columns].astype(str).agg("|".join, axis=1)
    hashed = [hashlib.sha256(s.encode("utf-8")).hexdigest() for s in concat]
    return pd.Series(hashed, index=df.index)

def write_iceberg(df: pd.DataFrame, stage: str, mode: str = "append") -> None:
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    _to_iceberg_with_retry(df=df, table=table, location=location, mode=mode)

def execute_athena_sql(sql: str, description: str) -> None:
    athena = _BOTO3_SESSION.client("athena")
    logger = get_logger("athena_sql")
    print(f"Athena SQL: {description}")

    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            logger.info("Athena SQL succeeded: %s", description)
            return
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Athena SQL failed ({description}): {reason}")
        time.sleep(2)

def _read_one_csv_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    return pd.read_csv(body)

def read_csv_files_from_s3(bucket: str, prefix: str) -> pd.DataFrame:
    s3 = _BOTO3_SESSION.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    if not keys:
        return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_S3_READS) as pool:
        frames = list(pool.map(lambda key: _read_one_csv_from_s3(s3, bucket, key), keys))
    return pd.concat(frames, ignore_index=True)

def _iceberg_row_count(table: str) -> int:
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=f'SELECT COUNT(*) AS cnt FROM "{GLUE_DATABASE}"."{table}"',
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Row count failed for {table}")
        time.sleep(2)

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])

def get_table_parameter(table_id: int, parameter_name: str, default: str = None) -> str:
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=(
            f'SELECT parameter_value FROM "{GLUE_DATABASE}"."table_parameters" '
            f"WHERE table_id = {int(table_id)} AND parameter_name = '{parameter_name}'"
        ),
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Failed to read table_parameters[{parameter_name}] for table_id={table_id}")
        time.sleep(2)

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    if len(rows) < 2:
        if default is not None:
            return default
        raise KeyError(f"No table_parameters row for table_id={table_id}, parameter_name={parameter_name!r}")
    return rows[1]["Data"][0]["VarCharValue"]

def get_watermark(table_id: int, default: str = None) -> str:
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=(
            f'SELECT last_watermark_value FROM "{GLUE_DATABASE}"."table_watermarks" '
            f"WHERE table_id = {int(table_id)}"
        ),
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Failed to read table_watermarks for table_id={table_id}")
        time.sleep(2)

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    if len(rows) < 2:
        if default is not None:
            return default
        raise KeyError(f"No table_watermarks row for table_id={table_id}")
    value = rows[1]["Data"][0].get("VarCharValue")
    return value if value is not None else default

def write_by_load_type(df: pd.DataFrame, stage: str, table_id: int = 1) -> None:
    load_type = get_table_parameter(table_id, "load_type", default="FULL").strip().upper()
    logger = get_logger("write_by_load_type")

    if load_type == "FULL":
        logger.info("table_id=%d load_type=FULL -> overwrite_iceberg(%s)", table_id, stage)
        overwrite_iceberg(df, stage)
    elif load_type in ("MERGE", "INCREMENTAL", "UPSERT"):
        logger.info("table_id=%d load_type=%s -> merge_iceberg(%s)", table_id, load_type, stage)
        merge_iceberg(df, stage)
    else:
        raise ValueError(
            f"Unknown load_type {load_type!r} in table_parameters for table_id={table_id} "
            f"(stage={stage!r}) - expected 'FULL' or 'MERGE'/'INCREMENTAL'/'UPSERT'"
        )

def merge_iceberg(df: pd.DataFrame, stage: str, key_column: str = "surrogate_key") -> None:
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    staging_table = f"{table}_merge_scratch"
    staging_location = location.rstrip("/") + "_merge_scratch/"

    logger = get_logger("merge_iceberg")

    if not _iceberg_table_exists(table):
        print(f"{table} BEFORE: table does not exist yet (0 rows)")
        logger.info("%s doesn't exist yet - writing initial data directly (no MERGE needed on a first run)", table)
        _to_iceberg_with_retry(df=df, table=table, location=location, mode="append")
        print(f"{table} AFTER:  {_iceberg_row_count(table)} row(s) (initial write, no merge)")
        return

    before_count = _iceberg_row_count(table)
    print(f"{table} BEFORE merge: {before_count} row(s)")

    _ensure_iceberg_columns(table, df)

    if _iceberg_table_exists(staging_table):
        _drop_iceberg_table(staging_table, staging_location)

    _to_iceberg_with_retry(
        df=df, table=staging_table, location=staging_location, mode="overwrite"
    )

    all_cols = [c for c in df.columns if c != key_column]
    update_set = ", ".join(f"{c} = s.{c}" for c in all_cols)
    insert_cols = ", ".join([key_column] + all_cols)
    insert_vals = ", ".join([f"s.{key_column}"] + [f"s.{c}" for c in all_cols])

    merge_sql = f"""
    MERGE INTO {GLUE_DATABASE}.{table} t
    USING {GLUE_DATABASE}.{staging_table} s
    ON t.{key_column} = s.{key_column}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    execute_athena_sql(merge_sql, f"Idempotent upsert into {table} ({len(df)} rows) via {staging_table}")

    after_count = _iceberg_row_count(table)
    print(f"{table} AFTER merge:  {after_count} row(s) (was {before_count}, delta {after_count - before_count:+d})")

def _drop_iceberg_table(table: str, location: str) -> None:
    glue = _BOTO3_SESSION.client("glue")
    try:
        glue.delete_table(DatabaseName=GLUE_DATABASE, Name=table)
    except glue.exceptions.EntityNotFoundException:
        pass

    s3 = _BOTO3_SESSION.client("s3")
    bucket, _, prefix = location.replace("s3://", "", 1).partition("/")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})

def overwrite_iceberg(df: pd.DataFrame, stage: str) -> None:
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    logger = get_logger("overwrite_iceberg")

    before_exists = _iceberg_table_exists(table)
    before_count = _iceberg_row_count(table) if before_exists else 0
    print(f"{table} BEFORE: {before_count} row(s)" if before_exists else f"{table} BEFORE: table does not exist yet (0 rows)")

    if before_exists:
        logger.info("Dropping %s (Glue table + S3 data) before full reload, so its schema always matches this run's dataframe exactly", table)
        _drop_iceberg_table(table, location)

    _to_iceberg_with_retry(df=df, table=table, location=location, mode="overwrite")

    after_count = _iceberg_row_count(table)
    print(f"{table} AFTER:  {after_count} row(s) (full reload, was {before_count})")

def read_iceberg(stage: str) -> pd.DataFrame:
    table, _ = TABLES[stage]
    return athena_read_sql(f'SELECT * FROM "{table}"')

_ATHENA_TYPE_TO_PANDAS = {
    "integer": "int", "bigint": "int", "smallint": "int", "tinyint": "int",
    "double": "float", "float": "float", "real": "float", "decimal": "float",
    "boolean": "bool",
    "timestamp": "datetime", "date": "datetime",
}

def athena_read_sql(sql: str) -> pd.DataFrame:
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Athena SELECT failed: {reason}")
        time.sleep(2)

    columns = None
    col_types = None
    data_rows = []
    paginator = athena.get_paginator("get_query_results")
    for page_num, page in enumerate(paginator.paginate(QueryExecutionId=query_id)):
        result_set = page["ResultSet"]
        if columns is None:
            column_info = result_set["ResultSetMetadata"]["ColumnInfo"]
            columns = [c["Name"] for c in column_info]
            col_types = [c["Type"] for c in column_info]
        rows = result_set["Rows"]
        start = 1 if page_num == 0 else 0
        for row in rows[start:]:
            data_rows.append([cell.get("VarCharValue") for cell in row["Data"]])

    df = pd.DataFrame(data_rows, columns=columns or [])
    for col, athena_type in zip(columns or [], col_types or []):
        kind = _ATHENA_TYPE_TO_PANDAS.get(athena_type)
        if kind == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif kind == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif kind == "bool":
            df[col] = df[col].map({"true": True, "false": False})
        elif kind == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def route_to_failed(df: pd.DataFrame, reason: str, stage: str) -> None:
    if df is None or df.empty:
        return
    tagged = df.copy()
    tagged["_failure_reason"] = reason
    tagged["_failed_stage"] = stage
    tagged["_failed_at"] = datetime.utcnow().isoformat()
    write_iceberg(tagged, "failed")

def record_audit(job_name: str, stage: str, rows_in: int, rows_out: int, rows_rejected: int,
                  start_time: datetime = None) -> None:
    now = datetime.utcnow()
    duration_seconds = round((now - start_time).total_seconds(), 3) if start_time is not None else None
    audit_row = pd.DataFrame([{
        "run_id": str(uuid.uuid4()),
        "job_name": job_name,
        "stage": stage,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "run_timestamp": now.isoformat(),
        "duration_seconds": duration_seconds,
    }])
    try:
        write_iceberg(audit_row, stage="audit", mode="append")
    except Exception as exc:
        get_logger("record_audit").warning(
            "record_audit() failed to write audit_iceberg (non-fatal, job continues): %s", exc
        )

def get_secret(secret_id: str, default: str = None) -> str:
    secrets = _BOTO3_SESSION.client("secretsmanager")
    try:
        response = secrets.get_secret_value(SecretId=secret_id)
        return response.get("SecretString", default)
    except Exception:
        if default is not None:
            return default
        raise

def send_alert(subject: str, message: str) -> None:
    sns = _BOTO3_SESSION.client("sns")
    topic_arn = SNS_TOPIC_ARN_OVERRIDE or f"arn:aws:sns:{AWS_REGION}:{get_account_id()}:{SNS_TOPIC_NAME}"
    sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    print(f"Alert sent: {subject}")
