import sys
import boto3
import time
import logging
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "05_ETL"))
from config import LOOKBACK_WINDOW_DAYS, NATURAL_KEY_COLUMNS, SOURCE_S3_BUCKET, SOURCE_S3_PREFIX

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="HDB Housing Metadata Iceberg Setup")
parser.add_argument("--database", required=True, help="Glue/Athena database name")
parser.add_argument("--workgroup", required=True, help="Athena workgroup")
parser.add_argument("--metadata-bucket", required=True, help="S3 metadata bucket")
parser.add_argument("--region", required=True, help="AWS region")
args = parser.parse_args()

DATABASE = args.database
WORKGROUP = args.workgroup
METADATA_BUCKET = args.metadata_bucket
REGION = args.region

athena = boto3.client("athena", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

def table_exists(table_name: str) -> bool:
    try:
        glue.get_table(DatabaseName=DATABASE, Name=table_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

def database_exists() -> bool:
    try:
        glue.get_database(Name=DATABASE)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

METADATA_LOCATION = f"s3://{METADATA_BUCKET}/metadata_iceberg_table"

def run_query(sql, description):
    logger.info(f"Running query: {description}")
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": f"s3://{METADATA_BUCKET}/athena-results/"},
        WorkGroup=WORKGROUP
    )
    query_execution_id = response["QueryExecutionId"]
    logger.info(f"Query started: {query_execution_id}")
    return query_execution_id

def wait_for_query(query_execution_id):
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            logger.info(f"Query succeeded: {query_execution_id}")
            return True
        elif status in ["FAILED", "CANCELLED"]:
            reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            logger.error(f"Query failed: {reason}")
            raise Exception(reason)

        time.sleep(2)

def execute_query(sql, description):
    query_id = run_query(sql, description)
    wait_for_query(query_id)

    time.sleep(5)

def setup_metadata():
    logger.info("============================================================")
    logger.info("HDB Housing Metadata Iceberg Setup Started")
    logger.info(f"Database         : {DATABASE}")
    logger.info(f"Athena Workgroup : {WORKGROUP}")
    logger.info(f"Metadata Bucket  : {METADATA_BUCKET}")
    logger.info(f"Metadata Location: {METADATA_LOCATION}")
    logger.info("============================================================")

    create_database_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {DATABASE}
    """

    if database_exists():
        logger.info(f"Database already exists, skipping: {DATABASE}")
    else:
        execute_query(create_database_sql, "Create HDB database")

    metadata_tables_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.metadata_tables (

        table_id        BIGINT,
        table_name      STRING,
        source_system   STRING,
        source_schema   STRING,
        source_table    STRING,
        source_path     STRING,
        target_layer    STRING,
        bronze_schema   STRING,
        silver_schema   STRING,
        gold_schema     STRING,
        active_flag     BOOLEAN,
        load_order      BIGINT,
        created_at      TIMESTAMP

    )
    LOCATION '{METADATA_LOCATION}/metadata_tables/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    metadata_tables_existed = table_exists("metadata_tables")
    if metadata_tables_existed:
        logger.info("Table already exists, skipping: metadata_tables")
    else:
        execute_query(metadata_tables_sql, "Create metadata_tables")

    table_parameters_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.table_parameters (

        table_id        BIGINT,
        parameter_name  STRING,
        parameter_value STRING,
        created_at      TIMESTAMP

    )
    LOCATION '{METADATA_LOCATION}/table_parameters/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    table_parameters_existed = table_exists("table_parameters")
    if table_parameters_existed:
        logger.info("Table already exists, skipping: table_parameters")
    else:
        execute_query(table_parameters_sql, "Create table_parameters")

    table_watermarks_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.table_watermarks (

        table_id             BIGINT,
        last_watermark_value STRING,
        last_updated_at      TIMESTAMP,
        last_run_id          BIGINT

    )
    PARTITIONED BY (table_id)
    LOCATION '{METADATA_LOCATION}/table_watermarks/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    table_watermarks_existed = table_exists("table_watermarks")
    if table_watermarks_existed:
        logger.info("Table already exists, skipping: table_watermarks")
    else:
        execute_query(table_watermarks_sql, "Create table_watermarks")

    pipeline_runs_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.pipeline_runs (

        run_id              BIGINT,
        table_id            BIGINT,
        layer               STRING,
        start_time          TIMESTAMP,
        end_time            TIMESTAMP,
        status              STRING,
        number_of_records   BIGINT,
        error_message       STRING

    )
    PARTITIONED BY (table_id)
    LOCATION '{METADATA_LOCATION}/pipeline_runs/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    if table_exists("pipeline_runs"):
        logger.info("Table already exists, skipping: pipeline_runs")
    else:
        execute_query(pipeline_runs_sql, "Create pipeline_runs")

    if table_parameters_existed:
        execute_query(
            f"DELETE FROM {DATABASE}.table_parameters WHERE table_id = 1",
            "Clear existing resaleflat_price table parameters before re-sync",
        )

    table_parameters_insert_sql = f"""
    INSERT INTO {DATABASE}.table_parameters
    VALUES

    (
        1,
        'load_type',
        'FULL',
        current_timestamp
    ),

    (
        1,
        'primary_key',
        '{",".join(NATURAL_KEY_COLUMNS)}',
        current_timestamp
    ),

    (
        1,
        'lookback_window',
        '{LOOKBACK_WINDOW_DAYS}',
        current_timestamp
    )
    """

    execute_query(table_parameters_insert_sql, "Sync resaleflat_price table parameters")

    table_watermarks_insert_sql = f"""
    INSERT INTO {DATABASE}.table_watermarks
    VALUES

    (
        1,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        2,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        3,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        5,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        6,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    )
    """

    if table_watermarks_existed:
        logger.info("table_watermarks already had data, skipping seed insert.")
    else:
        execute_query(table_watermarks_insert_sql, "Initialize table watermarks")

    source_path = f"s3://{SOURCE_S3_BUCKET}/{SOURCE_S3_PREFIX}/"

    if metadata_tables_existed:
        execute_query(
            f"DELETE FROM {DATABASE}.metadata_tables WHERE table_id = 1",
            "Clear existing resaleflat_price metadata before re-sync",
        )

    resaleflat_price_sql = f"""
    INSERT INTO {DATABASE}.metadata_tables
    VALUES (

        1,
        'resaleflat_price',
        'HDB',
        NULL,
        'resaleflat_price',
        '{source_path}',
        'gold',
        'raw_iceberg',
        'cleaned_iceberg,transformed_iceberg',
        'hashed_iceberg',
        TRUE,
        1,
        current_timestamp

    )
    """

    execute_query(resaleflat_price_sql, "Sync resaleflat_price metadata")

    logger.info("============================================================")
    logger.info("HDB Housing Metadata Setup Completed Successfully")
    logger.info("============================================================")

    print("Metadata tables setup completed successfully.")

if __name__ == "__main__":
    try:
        setup_metadata()
    except Exception:
        logger.exception("HDB Housing metadata setup failed")
        raise
