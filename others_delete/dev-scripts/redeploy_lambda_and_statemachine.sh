#!/usr/bin/env bash
# Redeploys ONLY the Lambda function code + the Step Functions state
# machine definition - the two files just changed (lambda_function.py's
# _clean_error_message() fix, build_state_machine_definition.py's
# CompactIcebergTables retention override + clean-error wiring). Does NOT
# touch buckets, IAM roles, Glue jobs, EventBridge rules, or the SNS
# topic - everything else setup.sh manages is left exactly as it is.
#
# Run from the project root (same directory as setup.sh).
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"
REGION="us-east-1"
AWS_PROFILE="${AWS_PROFILE:-sujen}"
SCRIPT_DIR="$(pwd)"

ACCOUNT_ID="$(aws sts get-caller-identity --profile "${AWS_PROFILE}" --query Account --output text)"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"
LAMBDA_SOURCE="${SCRIPT_DIR}/pipeline-scripts/01_template_creation/lambda-script"
LAMBDA_ZIP="${SCRIPT_DIR}/pipeline-scripts/lambda_function.zip"

STATE_MACHINE_NAME="${PROJECT_NAME}-pipeline"
STATE_MACHINE_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
STEPFUNCTIONS_ROLE_NAME="${PROJECT_NAME}-stepfunctions-role"
SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"
BUILD_STATE_MACHINE_SCRIPT="${SCRIPT_DIR}/pipeline-scripts/05_ETL/build_state_machine_definition.py"
STATE_MACHINE_DEFINITION_FILE="${SCRIPT_DIR}/pipeline-scripts/state_machine_definition.json"

GLUE_JOB_COMPACT="hdb-job-0-compact-metadata"
GLUE_JOB_2="hdb-job-2-raw-iceberg"
GLUE_JOB_2B="hdb-job-2b-data-profiling"
GLUE_JOB_3="hdb-job-3-cleaned-iceberg"
GLUE_JOB_4="hdb-job-4-transformed-iceberg"
GLUE_JOB_5="hdb-job-5-hashed-iceberg"

# Same optional-SES logic as setup.sh's Step 11B - unchanged, just reused
# here so this script builds the SAME kind of state machine (SES or SNS)
# your last full setup.sh run produced.
SES_RECIPIENT_EMAILS="${HDB_ALERT_RECIPIENT_EMAILS:-}"
SES_SENDER_EMAIL=""
if [[ -n "${SES_RECIPIENT_EMAILS}" ]]; then
    SES_SENDER_EMAIL="${HDB_SES_SENDER_EMAIL:-${SES_RECIPIENT_EMAILS%%,*}}"
fi

echo "=== 1) Redeploying Lambda code only (${LAMBDA_FUNCTION_NAME}) ==="

rm -f "${LAMBDA_ZIP}"
python - "${LAMBDA_SOURCE}" "${LAMBDA_ZIP}" <<'PYEOF'
import sys, zipfile
from pathlib import Path
source_dir = Path(sys.argv[1]); zip_path = Path(sys.argv[2])
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(source_dir)
        if "__pycache__" in rel.parts or path.suffix == ".pyc" or path.name == ".DS_Store":
            continue
        zf.write(path, rel)
PYEOF

if command -v cygpath >/dev/null 2>&1; then
    LAMBDA_ZIP_FOR_CLI="$(cygpath -w "${LAMBDA_ZIP}")"
else
    LAMBDA_ZIP_FOR_CLI="${LAMBDA_ZIP}"
fi

aws lambda update-function-code \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --zip-file "fileb://${LAMBDA_ZIP_FOR_CLI}" \
    >/dev/null

aws lambda wait function-updated-v2 \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}"

echo "Lambda code updated."
rm -f "${LAMBDA_ZIP}"

echo
echo "=== 2) Rebuilding + redeploying the state machine definition only (${STATE_MACHINE_NAME}) ==="

LAMBDA_ARN="$(
    aws lambda get-function \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Configuration.FunctionArn --output text
)"

SNS_TOPIC_ARN="$(
    aws sns create-topic \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --name "${SNS_TOPIC_NAME}" \
        --query TopicArn --output text
)"  # create-topic is idempotent - just resolves the existing ARN, doesn't touch subscriptions

STEPFUNCTIONS_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --query Role.Arn --output text
)"

python "${BUILD_STATE_MACHINE_SCRIPT}" \
    "${SNS_TOPIC_ARN}" \
    "${LAMBDA_ARN}" \
    "${GLUE_JOB_COMPACT}" \
    "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}" \
    "${SES_SENDER_EMAIL}" "${SES_RECIPIENT_EMAILS}" \
    "${STATE_MACHINE_DEFINITION_FILE}"

if [[ ! -s "${STATE_MACHINE_DEFINITION_FILE}" ]]; then
    echo "ERROR: State machine definition was not generated."
    exit 1
fi

if command -v cygpath >/dev/null 2>&1; then
    STATE_MACHINE_DEFINITION_FOR_CLI="$(cygpath -w "${STATE_MACHINE_DEFINITION_FILE}")"
else
    STATE_MACHINE_DEFINITION_FOR_CLI="${STATE_MACHINE_DEFINITION_FILE}"
fi

aws stepfunctions update-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    --definition "file://${STATE_MACHINE_DEFINITION_FOR_CLI}" \
    --role-arn "${STEPFUNCTIONS_ROLE_ARN}" \
    >/dev/null

rm -f "${STATE_MACHINE_DEFINITION_FILE}"

echo "State machine updated."
echo
echo "Done - only the Lambda code and the state machine definition were touched."
