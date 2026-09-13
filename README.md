# HDB Resale Flat Price Pipeline

Setup notes for the Iceberg-based ETL pipeline (ingestion → raw → cleaned → transformed → hashed).

## Prerequisites

- AWS CLI installed and configured with a profile that can create S3 buckets, Glue jobs/databases, a Lambda function, IAM roles, an EventBridge rule, an SNS topic, and a Step Functions state machine.
- Python 3 available as `python`, with `requirements.txt` (at the project root) installable via pip.
- Run from a bash shell (Git Bash / MINGW64 on Windows works — the script handles Windows paths via `cygpath` automatically).

## Quick start

From the project root:

    export AWS_PROFILE=your-profile-name   # defaults to "sujen" if unset
    bash pipeline-scripts/infrastructure_files/03_bash_scripts/setup.sh

This is idempotent — buckets, roles, and jobs that already exist are updated in place rather than duplicated, so it's safe to re-run any time you've changed a job script (it re-uploads `pipeline-scripts/` to S3 and points the Glue jobs at the refreshed code). If a step fails partway, it automatically rolls back everything it just created via `tear_down.sh`, unless `HDB_ROLLBACK_ON_FAILURE=0` is set first.

## Optional: nicely formatted alert emails

By default, no email recipient is configured, so pipeline success/failure alerts go out as a **plain-text** SNS message (an ASCII table). The pipeline already has HTML email formatting built in — it just needs SES turned on to use it.

To enable it, set these two environment variables **before** running `setup.sh`:

    export HDB_ALERT_RECIPIENT_EMAILS="you@example.com"
    export HDB_SES_SENDER_EMAIL="you@example.com"   # or a verified sender identity
    bash pipeline-scripts/infrastructure_files/03_bash_scripts/setup.sh

  - `HDB_ALERT_RECIPIENT_EMAILS` accepts a comma-separated list for multiple recipients.
  - If `HDB_SES_SENDER_EMAIL` is omitted, it defaults to the first address in `HDB_ALERT_RECIPIENT_EMAILS`.

**Prerequisite:** the sender identity (email or domain) must be **verified in AWS SES** first, or sends will fail silently. If the account is still in the SES sandbox, recipient addresses need verifying too. Verify identities in the SES console before setting these variables.

Leaving these unset is completely fine — the pipeline runs exactly the same either way. This only changes how the completion email looks.

## Verifying a run

After the state machine completes, compare each job's real execution time (not the partial in-script `duration_seconds` reported in the pipeline_runs email, which stops before each job's final write):

    for job in hdb-job-2-raw-iceberg hdb-job-3-cleaned-iceberg hdb-job-4-transformed-iceberg hdb-job-5-hashed-iceberg; do
      echo "=== $job ==="
      aws glue get-job-runs --profile "$AWS_PROFILE" --job-name "$job" \
        --query "JobRuns[0:2].{State:JobRunState,Started:StartedOn,Completed:CompletedOn,ExecutionTime:ExecutionTime}" \
        --output table
    done

## Known open items

| Item | Status |
| --- | --- |
| job_2/3/4/5 duplicate `*_staging` write (each job wrote its output twice per run) | Fixed |
| job_3/4/5 re-read the entire historical upstream table every run (no watermark filter) — the real cause of run duration climbing over time | Open |
| `resale_identifier` (job_4) collides on ~15% of rows, discarding real transactions as "duplicates" | Open |
