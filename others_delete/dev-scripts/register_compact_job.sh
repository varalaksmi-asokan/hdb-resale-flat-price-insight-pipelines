#!/usr/bin/env bash
# Registers ONLY the hdb-job-0-compact-metadata Glue job (the missing
# piece behind "EntityNotFoundException: Failed to start job run due to
# missing metadata") - does not touch any other Glue job, bucket, IAM
# role, or the state machine. Run from the project root (same dir as
# setup.sh). The script file itself (compact_iceberg_metadata.py) is
# already sitting in S3 under 07_maintenance/ from the last full setup.sh
# run's Step 3 upload - this just registers the missing Glue Job resource
# that points at it.
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"
REGION="us-east-1"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

ACCOUNT_ID="$(aws sts get-caller-identity --profile "${AWS_PROFILE}" --query Account --output text)"
BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"
PIPELINE_BUCKET="${BUCKET_PREFIX}-pipeline-scripts"
PIPELINE_PREFIX="python-scripts"

GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"
GLUE_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --query Role.Arn --output text
)"

GLUE_JOB_COMPACT="hdb-job-0-compact-metadata"
SCRIPT_LOCATION="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/07_maintenance/compact_iceberg_metadata.py"
GLUE_TEMP_DIR="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/glue-temp/"
GLUE_EXTRA_PY_FILES="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/common.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/config.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/context_tracking.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/metadata_lambda.py"
GLUE_ADDITIONAL_PYTHON_MODULES="pandas,requests"

echo "Checking whether ${SCRIPT_LOCATION} actually exists in S3..."
if ! aws s3api head-object --profile "${AWS_PROFILE}" --region "${REGION}" \
    --bucket "${PIPELINE_BUCKET}" --key "${PIPELINE_PREFIX}/07_maintenance/compact_iceberg_metadata.py" \
    >/dev/null 2>&1
then
    echo "ERROR: ${SCRIPT_LOCATION} not found in S3."
    echo "Re-syncing pipeline-scripts/ to S3 first..."
    aws s3 cp \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        "./pipeline-scripts/" \
        "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
        --recursive --quiet \
        --exclude "*__pycache__*" --exclude "*.pyc" --exclude "*.ipynb_checkpoints*"
    echo "Synced."
fi

DEFAULT_ARGUMENTS="$(cat <<JSONEOF
{
  "--extra-py-files": "${GLUE_EXTRA_PY_FILES}",
  "--additional-python-modules": "${GLUE_ADDITIONAL_PYTHON_MODULES}",
  "--TempDir": "${GLUE_TEMP_DIR}"
}
JSONEOF
)"

if aws glue get-job --profile "${AWS_PROFILE}" --region "${REGION}" --job-name "${GLUE_JOB_COMPACT}" >/dev/null 2>&1; then
    echo "Job already exists - updating it instead."
    JOB_UPDATE="$(cat <<JSONEOF
{
  "Role": "${GLUE_ROLE_ARN}",
  "Command": {"Name": "pythonshell", "ScriptLocation": "${SCRIPT_LOCATION}", "PythonVersion": "3.9"},
  "DefaultArguments": ${DEFAULT_ARGUMENTS},
  "MaxCapacity": 1,
  "Timeout": 30
}
JSONEOF
)"
    aws glue update-job \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --job-name "${GLUE_JOB_COMPACT}" \
        --job-update "${JOB_UPDATE}" \
        >/dev/null
else
    aws glue create-job \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --name "${GLUE_JOB_COMPACT}" \
        --role "${GLUE_ROLE_ARN}" \
        --command "Name=pythonshell,ScriptLocation=${SCRIPT_LOCATION},PythonVersion=3.9" \
        --default-arguments "${DEFAULT_ARGUMENTS}" \
        --max-capacity 1 \
        --timeout 30 \
        >/dev/null
fi

echo "Registered."
aws glue get-job --profile "${AWS_PROFILE}" --region "${REGION}" --job-name "${GLUE_JOB_COMPACT}" \
    --query 'Job.[Name,Command.ScriptLocation,Role]' --output table
