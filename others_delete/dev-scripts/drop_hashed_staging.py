"""
One-off fix: fully wipe the stray S3 objects left under hashed_iceberg_staging
(and its dead pre-bugfix orphan hashed_iceberg_staging_staging), using the
CONFIRMED REAL bucket - not config.py's computed default, which we found
does not match reality on this machine (it computes an account-id-suffixed
name based on locally-resolved credentials, but the pipeline's Iceberg
tables were actually registered against a DIFFERENT account-id-suffixed
bucket - confirmed via `aws glue get-table` + a direct `aws s3 ls` against
that exact bucket, which succeeded).

Ground truth confirmed 2026-09-02:
    aws glue get-table --database-name hdb_eventdriven_database \
        --name hashed_iceberg --query "Table.StorageDescriptor.Location" \
        --output text
    -> s3://mission-hdb-eventdriven-544795558120-hashed/hashed_iceberg

    aws s3 ls s3://mission-hdb-eventdriven-544795558120-hashed/
    -> PRE hashed_iceberg/
       PRE hashed_iceberg_merge_scratch/
       PRE hashed_iceberg_staging/               <- the bloated one (89 snapshots)
       PRE hashed_iceberg_staging_merge_scratch/
       PRE hashed_iceberg_staging_staging/       <- dead pre-bugfix orphan

The Glue Catalog entry for hashed_iceberg_staging was already deregistered
by an earlier plain `DROP TABLE` - but that statement never touched the
underlying S3 objects (Athena's DROP TABLE only deregisters the Catalog
entry). This script deletes those leftover S3 objects directly, and does
the same for the dead hashed_iceberg_staging_staging orphan while we're at
it (harmless leftover from the OLD pre-bugfix scratch-table naming, never
collided with anything, but also never got cleaned up).

Run from THIS directory (05_ETL), in your own terminal:

    python drop_hashed_staging.py
"""
from common import _drop_iceberg_table, _iceberg_table_exists

REAL_BUCKET = "mission-hdb-eventdriven-544795558120-hashed"

TARGETS = [
    ("hashed_iceberg_staging", f"s3://{REAL_BUCKET}/hashed_iceberg_staging/"),
    ("hashed_iceberg_staging_staging", f"s3://{REAL_BUCKET}/hashed_iceberg_staging_staging/"),
]

for table_name, location in TARGETS:
    print(f"--- {table_name} ---")
    print(f"Location       : {location}")
    existed_before = _iceberg_table_exists(table_name)
    print(f"Exists before   : {existed_before}")

    _drop_iceberg_table(table_name, location)

    existed_after = _iceberg_table_exists(table_name)
    print(f"Exists after    : {existed_after}")

    if existed_after:
        print("WARNING: table still shows as existing after drop - investigate.")
    else:
        print("OK - Glue Catalog entry (if any) deregistered and S3 objects deleted.")
    print()

print("Done. Next job_5 run will recreate hashed_iceberg_staging from scratch (fresh snapshot history).")
