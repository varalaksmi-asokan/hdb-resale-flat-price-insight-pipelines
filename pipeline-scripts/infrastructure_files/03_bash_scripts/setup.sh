
export AWS_PROFILE="${AWS_PROFILE:-sujen}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_REGION}"

set -Eeuo pipefail

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"

REGION="${AWS_REGION}"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

ACCOUNT_ID=""

GLUE_DATABASE="${PROJECT_NAME//-/_}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"
GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"
EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"
STEPFUNCTIONS_ROLE_NAME="${PROJECT_NAME}-stepfunctions-role"

STATE_MACHINE_NAME="${PROJECT_NAME}-pipeline"

GLUE_JOB_1="hdb-job-1-ingestion-to-source"
GLUE_JOB_COMPACT="hdb-job-0-compact-metadata"
GLUE_JOB_2="hdb-job-2-raw-iceberg"
GLUE_JOB_2B="hdb-job-2b-data-profiling"
GLUE_JOB_3="hdb-job-3-cleaned-iceberg"
GLUE_JOB_4="hdb-job-4-transformed-iceberg"
GLUE_JOB_5="hdb-job-5-hashed-iceberg"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

SOURCE_MANUAL_UPLOAD_PREFIX="${HDB_SOURCE_MANUAL_UPLOAD_PREFIX:-resale-flat-prices-manual-upload}"

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"

PIPELINE_SOURCE_DIRECTORY="${PROJECT_ROOT}/pipeline-scripts"
PIPELINE_PREFIX="python-scripts"

LAMBDA_SOURCE="${PROJECT_ROOT}/pipeline-scripts/infrastructure_files/01_template_creation/lambda-script"
LAMBDA_ZIP="${PROJECT_ROOT}/pipeline-scripts/lambda_function.zip"

GITHUB_ORG_REPO="${GITHUB_ORG_REPO:-Varalaxmiurs/HDB-resleflat-price}"
GITHUB_ORG="${GITHUB_ORG_REPO%%/*}"
GITHUB_REPO_NAME="${GITHUB_ORG_REPO#*/}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"

GITHUB_OIDC_HOST="token.actions.githubusercontent.com"
GITHUB_OIDC_THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea1"
GHA_ROLE_NAME="${GITHUB_ACTIONS_ROLE_NAME:-hdb-pipeline-github-actions}"

EXPECTED_METADATA_TABLES=(
    "metadata_tables"
    "pipeline_runs"
    "table_parameters"
    "table_watermarks"
)

ROLLBACK_ON_FAILURE="${HDB_ROLLBACK_ON_FAILURE:-1}"

rollback() {
    echo ""
    echo "============================================================"
    echo "ROLLING BACK - removing everything this run created"
    echo "============================================================"
    echo "PROJECT_NAME : ${PROJECT_NAME}"
    echo "AWS_REGION   : ${REGION}"
    echo ""

    if HDB_SKIP_CONFIRM=1 PROJECT_NAME="${PROJECT_NAME}" AWS_REGION="${REGION}"         AWS_PROFILE="${AWS_PROFILE}" bash "${SCRIPT_DIR}/tear_down.sh"
    then
        echo ""
        echo "============================================================"
        echo "ROLLBACK COMPLETE - environment is clean, nothing left behind."
        echo "============================================================"
    else
        echo ""
        echo "============================================================"
        echo "ROLLBACK FAILED - some resources may still exist."
        echo "Run tear_down.sh by hand to finish cleaning up."
        echo "============================================================"
    fi
}

on_error() {
    local exit_code=$?
    local failed_line="$1"

    echo ""
    echo "============================================================"
    echo "SETUP FAILED at line ${failed_line} (exit code ${exit_code})"
    echo "============================================================"

    if [[ "${ROLLBACK_ON_FAILURE}" == "1" ]]; then
        rollback
    else
        echo "Rollback disabled (HDB_ROLLBACK_ON_FAILURE=0)."
        echo "Resources created so far are left in place for inspection."
    fi

    exit "${exit_code}"
}

trap 'on_error ${LINENO}' ERR

echo "============================================================"
echo "HDB EVENT-DRIVEN ICEBERG PIPELINE"
echo "============================================================"
echo ""
echo "Project Directory : ${PROJECT_ROOT}"
echo "AWS Profile       : ${AWS_PROFILE}"
echo "AWS Region        : ${REGION}"
echo ""

if ! command -v aws >/dev/null 2>&1; then
    echo "ERROR: AWS CLI is not installed."
    exit 1
fi

if ! command -v python >/dev/null 2>&1; then
    echo "ERROR: Python is not installed or not available as 'python'."
    exit 1
fi

REQUIREMENTS_FILE="${PROJECT_ROOT}/requirements.txt"

if [[ -f "${REQUIREMENTS_FILE}" ]]; then
    python -m pip install --quiet -r "${REQUIREMENTS_FILE}"
else
    echo "WARNING: requirements.txt not found at ${REQUIREMENTS_FILE} - skipping."
fi

echo "Checking AWS identity..."

ACCOUNT_ID="$(
    aws sts get-caller-identity \
        --profile "${AWS_PROFILE}" \
        --query Account \
        --output text \
        --region "${REGION}"
)"

if [[ -z "${ACCOUNT_ID}" || "${ACCOUNT_ID}" == "None" ]]; then
    echo "ERROR: Unable to determine AWS Account ID."
    exit 1
fi

echo "AWS Account       : ${ACCOUNT_ID}"
echo "AWS Region        : ${REGION}"
echo ""

BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"

SOURCE_BUCKET="${BUCKET_PREFIX}-source"
RAW_BUCKET="${BUCKET_PREFIX}-raw"
CLEANED_BUCKET="${BUCKET_PREFIX}-cleaned"
TRANSFORMED_BUCKET="${BUCKET_PREFIX}-transformed"
HASHED_BUCKET="${BUCKET_PREFIX}-hashed"
FAILED_BUCKET="${BUCKET_PREFIX}-failed"
PIPELINE_BUCKET="${BUCKET_PREFIX}-pipeline-scripts"
AUDIT_BUCKET="${BUCKET_PREFIX}-audit-tables"

BUCKETS_CREATED=0
BUCKETS_EXISTED=0

create_bucket() {

    local BUCKET_NAME="$1"

    EXISTING_BUCKET="$(
        aws s3api list-buckets \
            --profile "${AWS_PROFILE}" \
            --query "Buckets[?Name=='${BUCKET_NAME}'].Name" \
            --output text
    )"

    if [[ -n "${EXISTING_BUCKET}" && "${EXISTING_BUCKET}" != "None" ]]; then

        EXISTING_REGION="$(
            aws s3api get-bucket-location \
                --profile "${AWS_PROFILE}" \
                --bucket "${BUCKET_NAME}" \
                --query LocationConstraint \
                --output text 2>/dev/null || true
        )"

        if [[ "${EXISTING_REGION}" == "None" || -z "${EXISTING_REGION}" ]]; then
            EXISTING_REGION="us-east-1"
        fi

        if [[ "${EXISTING_REGION}" != "${REGION}" ]]; then
            echo "ERROR: Bucket ${BUCKET_NAME} already exists in ${EXISTING_REGION},"
            echo "but this deployment requires ${REGION}."
            exit 1
        fi

        BUCKETS_EXISTED=$((BUCKETS_EXISTED + 1))

    else

        if [[ "${REGION}" == "us-east-1" ]]; then

            aws s3api create-bucket \
                --profile "${AWS_PROFILE}" \
                --bucket "${BUCKET_NAME}" \
                --region "${REGION}" \
                >/dev/null

        else

            aws s3api create-bucket \
                --profile "${AWS_PROFILE}" \
                --bucket "${BUCKET_NAME}" \
                --region "${REGION}" \
                --create-bucket-configuration \
                    "LocationConstraint=${REGION}" \
                >/dev/null

        fi

        BUCKETS_CREATED=$((BUCKETS_CREATED + 1))

    fi
}

create_bucket "${SOURCE_BUCKET}"
create_bucket "${RAW_BUCKET}"
create_bucket "${CLEANED_BUCKET}"
create_bucket "${TRANSFORMED_BUCKET}"
create_bucket "${HASHED_BUCKET}"
create_bucket "${FAILED_BUCKET}"
create_bucket "${PIPELINE_BUCKET}"
create_bucket "${AUDIT_BUCKET}"

create_prefix() {

    local BUCKET_NAME="$1"
    local KEY="$2"

    aws s3api put-object \
        --profile "${AWS_PROFILE}" \
        --bucket "${BUCKET_NAME}" \
        --key "${KEY}" \
        --region "${REGION}" \
        >/dev/null
}

create_prefix "${SOURCE_BUCKET}" "${SOURCE_MANUAL_UPLOAD_PREFIX}/"

# Pre-create the alert-logs/ folders in the audit bucket so they're visible
# in the console right away. NOTE: this is cosmetic only - S3 has no real
# folders, and put_object never needs a prefix to pre-exist, so this does
# NOT fix (and was never the cause of) the Lambda's per-run alert-logs
# writes not landing. It just makes the empty folder show up ahead of time.
create_prefix "${AUDIT_BUCKET}" "alert-logs/success/"
create_prefix "${AUDIT_BUCKET}" "alert-logs/failure/"

if [[ ! -d "${PIPELINE_SOURCE_DIRECTORY}" ]]; then

    echo "ERROR: Pipeline scripts directory not found:"
    echo "${PIPELINE_SOURCE_DIRECTORY}"

    exit 1
fi

aws s3api put-object \
    --profile "${AWS_PROFILE}" \
    --bucket "${PIPELINE_BUCKET}" \
    --key "${PIPELINE_PREFIX}/" \
    --region "${REGION}" \
    >/dev/null

aws s3 cp \
    --profile "${AWS_PROFILE}" \
    "${PIPELINE_SOURCE_DIRECTORY}/" \
    "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
    --recursive \
    --quiet \
    --exclude "*__pycache__*" \
    --exclude "*.pyc" \
    --exclude "*.ipynb_checkpoints*" \
    --region "${REGION}"

FILE_COUNT="$(
    aws s3 ls \
        --profile "${AWS_PROFILE}" \
        "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
        --recursive \
        --region "${REGION}" |
        wc -l
)"

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then
    :
else

    aws glue create-database \
        --profile "${AWS_PROFILE}" \
        --database-input \
        "Name=${GLUE_DATABASE},Description=HDB Event Driven Iceberg Database" \
        --region "${REGION}" \
        >/dev/null

fi

METADATA_SCRIPT="${PROJECT_ROOT}/pipeline-scripts/infrastructure_files/02_meta_datasetup/01_metadata_setup.py"

if [[ ! -f "${METADATA_SCRIPT}" ]]; then

    echo "ERROR: Metadata setup script not found:"
    echo "${METADATA_SCRIPT}"

    exit 1
fi

python "${METADATA_SCRIPT}" \
    --database "${GLUE_DATABASE}" \
    --workgroup "${ATHENA_WORKGROUP}" \
    --metadata-bucket "${AUDIT_BUCKET}" \
    --region "${REGION}" \
    >/dev/null

SNS_TOPIC_ARN="$(
    aws sns create-topic \
        --profile "${AWS_PROFILE}" \
        --name "${SNS_TOPIC_NAME}" \
        --region "${REGION}" \
        --query TopicArn \
        --output text
)"

STATE_MACHINE_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"

SES_RECIPIENT_EMAILS="${HDB_ALERT_RECIPIENT_EMAILS:-}"
SES_SENDER_EMAIL=""
if [[ -n "${SES_RECIPIENT_EMAILS}" ]]; then
    SES_SENDER_EMAIL="${HDB_SES_SENDER_EMAIL:-${SES_RECIPIENT_EMAILS%%,*}}"
fi

LAMBDA_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"lambda.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --assume-role-policy-document "${LAMBDA_TRUST_POLICY}" \
        >/dev/null

    sleep 10
fi

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

LAMBDA_SES_POLICY_STATEMENT=""
if [[ -n "${SES_SENDER_EMAIL}" ]]; then
    LAMBDA_SES_POLICY_STATEMENT=',
    {
      "Sid": "SESSendFailureAlertOnly",
      "Effect": "Allow",
      "Action": "ses:SendEmail",
      "Resource": "arn:aws:ses:'"${REGION}"':'"${ACCOUNT_ID}"':identity/'"${SES_SENDER_EMAIL}"'"
    }'
fi

LAMBDA_SCOPED_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3AuditAndFailedBucketsOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::${AUDIT_BUCKET}",
        "arn:aws:s3:::${AUDIT_BUCKET}/*",
        "arn:aws:s3:::${FAILED_BUCKET}",
        "arn:aws:s3:::${FAILED_BUCKET}/*"
      ]
    },
    {
      "Sid": "AthenaQueryOnMetadataWorkgroupOnly",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ],
      "Resource": "arn:aws:athena:${REGION}:${ACCOUNT_ID}:workgroup/${ATHENA_WORKGROUP}"
    },
    {
      "Sid": "GlueCatalogThisDatabaseOnly",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
        "glue:GetPartitions", "glue:GetPartition",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:${REGION}:${ACCOUNT_ID}:catalog",
        "arn:aws:glue:${REGION}:${ACCOUNT_ID}:database/${GLUE_DATABASE}",
        "arn:aws:glue:${REGION}:${ACCOUNT_ID}:table/${GLUE_DATABASE}/*"
      ]
    },
    {
      "Sid": "ExecutionHistoryForFailureHandlingOnly",
      "Effect": "Allow",
      "Action": "states:GetExecutionHistory",
      "Resource": "arn:aws:states:${REGION}:${ACCOUNT_ID}:execution:${STATE_MACHINE_NAME}:*"
    },
    {
      "Sid": "SNSPublishFailureAlertOnly",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${SNS_TOPIC_ARN}"
    }${LAMBDA_SES_POLICY_STATEMENT}
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-name "${LAMBDA_ROLE_NAME}-scoped-access" \
    --policy-document "${LAMBDA_SCOPED_POLICY}" \
    >/dev/null

LAMBDA_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

GLUE_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"glue.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --assume-role-policy-document "${GLUE_TRUST_POLICY}" \
        >/dev/null

fi

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

GLUE_SCOPED_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ProjectBucketsOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::${SOURCE_BUCKET}", "arn:aws:s3:::${SOURCE_BUCKET}/*",
        "arn:aws:s3:::${RAW_BUCKET}", "arn:aws:s3:::${RAW_BUCKET}/*",
        "arn:aws:s3:::${CLEANED_BUCKET}", "arn:aws:s3:::${CLEANED_BUCKET}/*",
        "arn:aws:s3:::${TRANSFORMED_BUCKET}", "arn:aws:s3:::${TRANSFORMED_BUCKET}/*",
        "arn:aws:s3:::${HASHED_BUCKET}", "arn:aws:s3:::${HASHED_BUCKET}/*",
        "arn:aws:s3:::${FAILED_BUCKET}", "arn:aws:s3:::${FAILED_BUCKET}/*",
        "arn:aws:s3:::${AUDIT_BUCKET}", "arn:aws:s3:::${AUDIT_BUCKET}/*",
        "arn:aws:s3:::${PIPELINE_BUCKET}", "arn:aws:s3:::${PIPELINE_BUCKET}/*"
      ]
    },
    {
      "Sid": "AthenaQueryOnPipelineWorkgroupOnly",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ],
      "Resource": "arn:aws:athena:${REGION}:${ACCOUNT_ID}:workgroup/${ATHENA_WORKGROUP}"
    },
    {
      "Sid": "SnsPublishOwnAlertsOnly",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${SNS_TOPIC_ARN}"
    }
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-name "${GLUE_ROLE_NAME}-scoped-access" \
    --policy-document "${GLUE_SCOPED_POLICY}" \
    >/dev/null

GLUE_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

GLUE_ADDITIONAL_PYTHON_MODULES="pandas,requests"
GLUE_PYTHON_MODULES_INSTALLER_OPTION=""

GLUE_TEMP_DIR="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/glue-temp/"
GLUE_EXTRA_PY_FILES="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/common.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/config.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/context_tracking.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/metadata_lambda.py"

create_or_update_glue_job() {

    local JOB_NAME="$1"
    local SCRIPT_FILE="$2"
    local SCRIPT_FOLDER="${3:-05_ETL}"
    local SCRIPT_LOCATION="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/${SCRIPT_FOLDER}/${SCRIPT_FILE}"

    local DEFAULT_ARGUMENTS
    DEFAULT_ARGUMENTS="$(cat <<JSONEOF
{
  "--extra-py-files": "${GLUE_EXTRA_PY_FILES}",
  "--additional-python-modules": "${GLUE_ADDITIONAL_PYTHON_MODULES}",
  "--TempDir": "${GLUE_TEMP_DIR}",
  "--HDB_PROJECT_NAME": "${PROJECT_NAME}"
}
JSONEOF
)"
    if [[ -n "${GLUE_PYTHON_MODULES_INSTALLER_OPTION}" ]]; then
        DEFAULT_ARGUMENTS="$(
            python3 -c "
import json, sys
args = json.loads(sys.argv[1])
args['--python-modules-installer-option'] = sys.argv[2]
print(json.dumps(args))
" "${DEFAULT_ARGUMENTS}" "${GLUE_PYTHON_MODULES_INSTALLER_OPTION}"
        )"
    fi

    if aws glue get-job \
        --profile "${AWS_PROFILE}" \
        --job-name "${JOB_NAME}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then

        local JOB_UPDATE
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
            --profile "${AWS_PROFILE}" \
            --job-name "${JOB_NAME}" \
            --job-update "${JOB_UPDATE}" \
            --region "${REGION}" \
            >/dev/null

    else

        aws glue create-job \
            --profile "${AWS_PROFILE}" \
            --name "${JOB_NAME}" \
            --role "${GLUE_ROLE_ARN}" \
            --command "Name=pythonshell,ScriptLocation=${SCRIPT_LOCATION},PythonVersion=3.9" \
            --default-arguments "${DEFAULT_ARGUMENTS}" \
            --max-capacity 1 \
            --timeout 30 \
            --region "${REGION}" \
            >/dev/null

    fi
}

create_or_update_glue_job "${GLUE_JOB_1}"  "job_1_ingestion_to_source.py"
create_or_update_glue_job "${GLUE_JOB_COMPACT}" "compact_iceberg_metadata.py" "07_maintenance"
create_or_update_glue_job "${GLUE_JOB_2}"  "job_2_raw_iceberg.py"
create_or_update_glue_job "${GLUE_JOB_2B}" "job_2b_data_profiling.py"
create_or_update_glue_job "${GLUE_JOB_3}"  "job_3_cleaned_iceberg.py"
create_or_update_glue_job "${GLUE_JOB_4}"  "job_4_transformed_iceberg.py"
create_or_update_glue_job "${GLUE_JOB_5}"  "job_5_hashed_iceberg.py"

EVENT_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"events.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --assume-role-policy-document "${EVENT_TRUST_POLICY}" \
        >/dev/null

fi

if [[ -n "${HDB_ALERT_RECIPIENT_EMAILS:-}" ]]; then
    python "${PROJECT_ROOT}/pipeline-scripts/infrastructure_files/04_json_scripts/sns_subscription_setup.py" --region "${REGION}"
else
    echo "Skipped: HDB_ALERT_RECIPIENT_EMAILS not set."
    echo "Run this later to subscribe an email for alerts:"
    echo "  python ${PROJECT_ROOT}/pipeline-scripts/infrastructure_files/04_json_scripts/sns_subscription_setup.py --region ${REGION} --email you@example.com"
fi

OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${GITHUB_OIDC_HOST}"

if aws iam list-open-id-connect-providers \
    --profile "${AWS_PROFILE}" \
    --query "OpenIDConnectProviderList[?Arn=='${OIDC_PROVIDER_ARN}'].Arn" \
    --output text |
    grep -q .
then
    :
else

    aws iam create-open-id-connect-provider \
        --profile "${AWS_PROFILE}" \
        --url "https://${GITHUB_OIDC_HOST}" \
        --client-id-list sts.amazonaws.com \
        --thumbprint-list "${GITHUB_OIDC_THUMBPRINT}" \
        >/dev/null

fi

GHA_TRUST_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "${OIDC_PROVIDER_ARN}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "${GITHUB_OIDC_HOST}:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "${GITHUB_OIDC_HOST}:sub": "repo:${GITHUB_ORG}*/${GITHUB_REPO_NAME}*:ref:refs/heads/${GITHUB_BRANCH}"
        }
      }
    }
  ]
}
JSON
)"

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    >/dev/null 2>&1
then
    aws iam update-assume-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --policy-document "${GHA_TRUST_POLICY}" \
        >/dev/null

else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --assume-role-policy-document "${GHA_TRUST_POLICY}" \
        >/dev/null

fi

GHA_S3_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${PIPELINE_BUCKET}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${PIPELINE_BUCKET}/*"
    }
  ]
}
JSON
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "GitHubActionsS3PipelineAccess" \
    --policy-document "${GHA_S3_POLICY}" \
    >/dev/null

GHA_GLUE_SNS_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJob"
      ],
      "Resource": "arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/hdb-job-*"
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${SNS_TOPIC_ARN}"
    }
  ]
}
JSON
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "${PROJECT_NAME}-github-actions-glue-sns" \
    --policy-document "${GHA_GLUE_SNS_POLICY}" \
    >/dev/null

# states:UpdateStateMachine requires the caller to also be able to pass the
# state machine's execution role to the Step Functions service - without the
# iam:PassRole statement below, this same CI role hits AccessDeniedException
# on iam:PassRole even though it's separately allowed to call
# UpdateStateMachine itself. Scoped via iam:PassedToService so this role can
# only hand that one role to Step Functions, not to anything else.
GHA_STEPFUNCTIONS_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "states:DescribeStateMachine",
        "states:UpdateStateMachine"
      ],
      "Resource": "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::${ACCOUNT_ID}:role/${STEPFUNCTIONS_ROLE_NAME}",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "states.amazonaws.com"
        }
      }
    }
  ]
}
JSON
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "${PROJECT_NAME}-github-actions-stepfunctions" \
    --policy-document "${GHA_STEPFUNCTIONS_POLICY}" \
    >/dev/null

GHA_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

if [[ ! -d "${LAMBDA_SOURCE}" ]]; then

    echo "ERROR: Lambda source directory not found:"
    echo "${LAMBDA_SOURCE}"

    exit 1
fi

if [[ ! -f "${LAMBDA_SOURCE}/lambda_function.py" ]]; then

    echo "ERROR: lambda_function.py not found:"
    echo "${LAMBDA_SOURCE}/lambda_function.py"

    exit 1
fi

find "${LAMBDA_SOURCE}" \
    -type f \
    ! -path "*/__pycache__/*" \
    ! -name "*.pyc" \
    ! -name ".DS_Store" \
    >/dev/null

rm -f "${LAMBDA_ZIP}"

python - "${LAMBDA_SOURCE}" "${LAMBDA_ZIP}" <<'PYEOF'
import sys
import zipfile
from pathlib import Path

source_dir = Path(sys.argv[1])
zip_path = Path(sys.argv[2])

with zipfile.ZipFile(
    zip_path,
    "w",
    zipfile.ZIP_DEFLATED
) as zf:

    for path in sorted(source_dir.rglob("*")):

        if not path.is_file():
            continue

        relative_path = path.relative_to(source_dir)

        if "__pycache__" in relative_path.parts:
            continue

        if path.suffix == ".pyc":
            continue

        if path.name == ".DS_Store":
            continue

        zf.write(path, relative_path)

PYEOF

if [[ ! -s "${LAMBDA_ZIP}" ]]; then
    echo "ERROR: Lambda ZIP was not created."
    exit 1
fi

if command -v cygpath >/dev/null 2>&1; then
    LAMBDA_ZIP_FOR_CLI="$(cygpath -w "${LAMBDA_ZIP}")"
else
    LAMBDA_ZIP_FOR_CLI="${LAMBDA_ZIP}"
fi

LAMBDA_ENV_JSON="$(python - "${REGION}" "${GLUE_DATABASE}" "${ATHENA_WORKGROUP}" "${AUDIT_BUCKET}" "${STATE_MACHINE_ARN}" "${SNS_TOPIC_ARN}" "${SES_SENDER_EMAIL}" "${SES_RECIPIENT_EMAILS}" <<'ENVPY'
import json
import sys

region, glue_db, workgroup, audit_bucket, sm_arn, sns_arn, ses_sender, ses_recipients = sys.argv[1:9]

print(json.dumps({"Variables": {
    "AWS_REGION_NAME": region,
    "GLUE_DATABASE": glue_db,
    "ATHENA_WORKGROUP": workgroup,
    "AUDIT_BUCKET": audit_bucket,
    "STATE_MACHINE_ARN": sm_arn,
    "SNS_TOPIC_ARN": sns_arn,
    "SES_SENDER_EMAIL": ses_sender,
    "SES_RECIPIENT_EMAILS": ses_recipients,
}}))
ENVPY
)"

if aws lambda get-function \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    >/dev/null 2>&1
then

    aws lambda update-function-code \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --zip-file "fileb://${LAMBDA_ZIP_FOR_CLI}" \
        >/dev/null

    aws lambda update-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.12 \
        --handler lambda_function.lambda_handler \
        --role "${LAMBDA_ROLE_ARN}" \
        --timeout 60 \
        --memory-size 512 \
        --environment "${LAMBDA_ENV_JSON}" \
        >/dev/null

else

    aws lambda create-function \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.12 \
        --role "${LAMBDA_ROLE_ARN}" \
        --handler lambda_function.lambda_handler \
        --timeout 60 \
        --memory-size 512 \
        --zip-file "fileb://${LAMBDA_ZIP_FOR_CLI}" \
        --environment "${LAMBDA_ENV_JSON}" \
        >/dev/null

fi

aws lambda wait function-active-v2 \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}"

LAMBDA_ARN="$(
    aws lambda get-function \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Configuration.FunctionArn \
        --output text
)"

GLUE_LAMBDA_INVOKE_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "${LAMBDA_ARN}"
    }
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-name "${GLUE_ROLE_NAME}-lambda-invoke-policy" \
    --policy-document "${GLUE_LAMBDA_INVOKE_POLICY}" \
    >/dev/null

aws s3api put-bucket-notification-configuration \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --bucket "${SOURCE_BUCKET}" \
    --notification-configuration '{"EventBridgeConfiguration": {}}' \
    >/dev/null

EVENT_PATTERN="$(cat <<JSONEOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {"name": ["${SOURCE_BUCKET}"]},
    "object": {"key": [{"prefix": "${SOURCE_MANUAL_UPLOAD_PREFIX}/"}]}
  }
}
JSONEOF
)"

aws events put-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${EVENT_RULE_NAME}" \
    --event-pattern "${EVENT_PATTERN}" \
    --state ENABLED \
    >/dev/null

STEPFUNCTIONS_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"states.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --assume-role-policy-document "${STEPFUNCTIONS_TRUST_POLICY}" \
        >/dev/null

fi

SES_POLICY_STATEMENT=""
if [[ -n "${SES_SENDER_EMAIL}" ]]; then
    SES_POLICY_STATEMENT=',
    {
      "Effect": "Allow",
      "Action": "ses:SendEmail",
      "Resource": "arn:aws:ses:'"${REGION}"':'"${ACCOUNT_ID}"':identity/'"${SES_SENDER_EMAIL}"'"
    }'
fi

STEPFUNCTIONS_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
      "Resource": "arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/hdb-job-*"
    },
    {
      "Effect": "Allow",
      "Action": ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
      "Resource": "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/StepFunctionsGetEventForGlueJobRule"
    },
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "${LAMBDA_ARN}"
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${SNS_TOPIC_ARN}"
    },
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::${AUDIT_BUCKET}/alert-logs/*"
    }${SES_POLICY_STATEMENT}
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
    --policy-name "${STEPFUNCTIONS_ROLE_NAME}-policy" \
    --policy-document "${STEPFUNCTIONS_POLICY}" \
    >/dev/null

STEPFUNCTIONS_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

STATE_MACHINE_DEFINITION_FILE="${SCRIPT_DIR}/.state_machine_definition.json"

BUILD_STATE_MACHINE_SCRIPT="${PROJECT_ROOT}/pipeline-scripts/05_ETL/build_state_machine_definition.py"

STATE_MACHINE_TIMEOUT_SECONDS="${HDB_STATE_MACHINE_TIMEOUT_SECONDS:-7200}"
export HDB_STATE_MACHINE_TIMEOUT_SECONDS="${STATE_MACHINE_TIMEOUT_SECONDS}"

python "${BUILD_STATE_MACHINE_SCRIPT}" \
    "${SNS_TOPIC_ARN}" \
    "${LAMBDA_ARN}" \
    "${GLUE_JOB_COMPACT}" \
    "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}" \
    "${SES_SENDER_EMAIL}" "${SES_RECIPIENT_EMAILS}" \
    "${AUDIT_BUCKET}" \
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

if aws stepfunctions describe-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    >/dev/null 2>&1
then

    aws stepfunctions update-state-machine \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --definition "file://${STATE_MACHINE_DEFINITION_FOR_CLI}" \
        --role-arn "${STEPFUNCTIONS_ROLE_ARN}" \
        >/dev/null

else

    aws stepfunctions create-state-machine \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --name "${STATE_MACHINE_NAME}" \
        --definition "file://${STATE_MACHINE_DEFINITION_FOR_CLI}" \
        --role-arn "${STEPFUNCTIONS_ROLE_ARN}" \
        --type STANDARD \
        >/dev/null

fi

rm -f "${STATE_MACHINE_DEFINITION_FILE}" 2>/dev/null || true

INGESTION_TRIGGER_RULE_NAME="${PROJECT_NAME}-ingestion-complete-trigger"

INGESTION_EVENT_PATTERN="$(cat <<JSONEOF
{
  "source": ["aws.glue"],
  "detail-type": ["Glue Job State Change"],
  "detail": {
    "jobName": ["${GLUE_JOB_1}"],
    "state": ["SUCCEEDED"]
  }
}
JSONEOF
)"

aws events put-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${INGESTION_TRIGGER_RULE_NAME}" \
    --event-pattern "${INGESTION_EVENT_PATTERN}" \
    --state ENABLED \
    >/dev/null

EVENT_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

EVENT_ROLE_SFN_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": "${STATE_MACHINE_ARN}"
    }
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    --policy-name "${EVENT_ROLE_NAME}-start-execution" \
    --policy-document "${EVENT_ROLE_SFN_POLICY}" \
    >/dev/null

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${INGESTION_TRIGGER_RULE_NAME}" \
    --targets "Id=start-pipeline-state-machine,Arn=${STATE_MACHINE_ARN},RoleArn=${EVENT_ROLE_ARN}" \
    >/dev/null

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${EVENT_RULE_NAME}" \
    --targets "Id=start-pipeline-state-machine,Arn=${STATE_MACHINE_ARN},RoleArn=${EVENT_ROLE_ARN}" \
    >/dev/null

TIMEOUT_ALERT_RULE_NAME="${PROJECT_NAME}-execution-timeout-alert"

TIMEOUT_EVENT_PATTERN="$(cat <<JSONEOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": ["${STATE_MACHINE_ARN}"],
    "status": ["TIMED_OUT"]
  }
}
JSONEOF
)"

aws events put-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${TIMEOUT_ALERT_RULE_NAME}" \
    --event-pattern "${TIMEOUT_EVENT_PATTERN}" \
    --state ENABLED \
    >/dev/null

TIMEOUT_ALERT_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${TIMEOUT_ALERT_RULE_NAME}"

CURRENT_SNS_POLICY="$(
    aws sns get-topic-attributes \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --topic-arn "${SNS_TOPIC_ARN}" \
        --query "Attributes.Policy" \
        --output text
)"

UPDATED_SNS_POLICY="$(python - "${CURRENT_SNS_POLICY}" "${SNS_TOPIC_ARN}" "${TIMEOUT_ALERT_RULE_ARN}" <<'POLICYPY'
import json
import sys

raw_policy, sns_arn, rule_arn = sys.argv[1], sys.argv[2], sys.argv[3]

try:
    policy = json.loads(raw_policy) if raw_policy and raw_policy != "None" else {}
except json.JSONDecodeError:
    policy = {}

policy.setdefault("Version", "2012-10-17")
statements = policy.setdefault("Statement", [])

sid = "AllowEventBridgeTimeoutAlertPublish"
statements[:] = [s for s in statements if s.get("Sid") != sid]
statements.append({
    "Sid": sid,
    "Effect": "Allow",
    "Principal": {"Service": "events.amazonaws.com"},
    "Action": "sns:Publish",
    "Resource": sns_arn,
    "Condition": {"ArnEquals": {"aws:SourceArn": rule_arn}},
})

print(json.dumps(policy))
POLICYPY
)"

aws sns set-topic-attributes \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --topic-arn "${SNS_TOPIC_ARN}" \
    --attribute-name Policy \
    --attribute-value "${UPDATED_SNS_POLICY}" \
    >/dev/null

TIMEOUT_ALERT_TARGETS="$(python - "${SNS_TOPIC_ARN}" "${STATE_MACHINE_TIMEOUT_SECONDS}" <<'TARGETPY'
import json
import sys

sns_arn, timeout_seconds = sys.argv[1], sys.argv[2]

message = (
    "HDB Resale Flat Prices Pipeline - execution TIMED OUT\n"
    "======================================================\n"
    "This run exceeded its TimeoutSeconds limit ({0}s / {1:.0f} min) and was "
    "force-stopped by Step Functions before it could reach its own "
    "failure-handling states - so no per-step pipeline_runs row was written "
    "for this run.\n\n"
    "Execution: <execArn>\n"
    "Started:   <start>\n"
    "Stopped:   <stop>\n\n"
    "Check this execution's history in the console, or via:\n"
    "aws stepfunctions describe-execution --execution-arn <execArn>"
).format(int(timeout_seconds), int(timeout_seconds) / 60)

targets = [{
    "Id": "notify-execution-timeout",
    "Arn": sns_arn,
    "InputTransformer": {
        "InputPathsMap": {
            "execArn": "$.detail.executionArn",
            "start": "$.detail.startDate",
            "stop": "$.detail.stopDate",
        },
        "InputTemplate": json.dumps(message),
    },
}]

print(json.dumps(targets))
TARGETPY
)"

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${TIMEOUT_ALERT_RULE_NAME}" \
    --targets "${TIMEOUT_ALERT_TARGETS}" \
    >/dev/null

FAILURE_ALERT_RULE_NAME="${PROJECT_NAME}-execution-failure-alert"

FAILURE_EVENT_PATTERN="$(cat <<JSONEOF
{
  "source": ["aws.states"],
  "detail-type": ["Step Functions Execution Status Change"],
  "detail": {
    "stateMachineArn": ["${STATE_MACHINE_ARN}"],
    "status": ["FAILED"]
  }
}
JSONEOF
)"

aws events put-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${FAILURE_ALERT_RULE_NAME}" \
    --event-pattern "${FAILURE_EVENT_PATTERN}" \
    --state ENABLED \
    >/dev/null

FAILURE_ALERT_RULE_ARN="arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${FAILURE_ALERT_RULE_NAME}"

aws lambda remove-permission \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --statement-id "${FAILURE_ALERT_RULE_NAME}-invoke" \
    >/dev/null 2>&1 || true

aws lambda add-permission \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --statement-id "${FAILURE_ALERT_RULE_NAME}-invoke" \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "${FAILURE_ALERT_RULE_ARN}" \
    >/dev/null

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${FAILURE_ALERT_RULE_NAME}" \
    --targets "Id=invoke-execution-failure-handler,Arn=${LAMBDA_ARN}" \
    >/dev/null

EXPECTED_BUCKETS=(
    "${SOURCE_BUCKET}"
    "${RAW_BUCKET}"
    "${CLEANED_BUCKET}"
    "${TRANSFORMED_BUCKET}"
    "${HASHED_BUCKET}"
    "${FAILED_BUCKET}"
    "${PIPELINE_BUCKET}"
    "${AUDIT_BUCKET}"
)

for bucket in "${EXPECTED_BUCKETS[@]}"; do

    if aws s3api list-buckets \
        --profile "${AWS_PROFILE}" \
        --query "Buckets[?Name=='${bucket}'].Name" \
        --output text |
        grep -Fxq "${bucket}"
    then
        :
    else

        echo "ERROR: Missing bucket: ${bucket}"
        exit 1

    fi

done

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: Glue database missing: ${GLUE_DATABASE}"
    exit 1

fi

for table in "${EXPECTED_METADATA_TABLES[@]}"; do

    if aws glue get-table \
        --profile "${AWS_PROFILE}" \
        --database-name "${GLUE_DATABASE}" \
        --name "${table}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then
        :
    else

        echo "ERROR: Missing Glue table: ${table}"
        exit 1

    fi

done

for job in "${GLUE_JOB_1}" "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}"; do

    if aws glue get-job \
        --profile "${AWS_PROFILE}" \
        --job-name "${job}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then
        :

    else

        echo "ERROR: Missing Glue job: ${job}"
        exit 1

    fi

done

if aws stepfunctions describe-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: Step Functions state machine missing: ${STATE_MACHINE_NAME}"
    exit 1

fi

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${INGESTION_TRIGGER_RULE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: Ingestion-complete trigger rule missing: ${INGESTION_TRIGGER_RULE_NAME}"
    exit 1

fi

LAMBDA_STATE="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query State \
        --output text
)"

if [[ "${LAMBDA_STATE}" == "Active" ]]; then
    :
else

    echo "ERROR: Lambda is not Active."
    echo "Current state: ${LAMBDA_STATE}"
    exit 1

fi

LAMBDA_HANDLER="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Handler \
        --output text
)"

LAMBDA_TIMEOUT="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Timeout \
        --output text
)"

if [[ "${LAMBDA_HANDLER}" != "lambda_function.lambda_handler" ]]; then
    echo "ERROR: Unexpected Lambda handler: ${LAMBDA_HANDLER}"
    exit 1
fi

if [[ "${LAMBDA_TIMEOUT}" -lt 60 ]]; then
    echo "ERROR: Lambda timeout is too low: ${LAMBDA_TIMEOUT}"
    exit 1
fi

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${EVENT_RULE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: EventBridge rule missing: ${EVENT_RULE_NAME}"
    exit 1

fi

TARGET_COUNT="$(
    aws events list-targets-by-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --rule "${EVENT_RULE_NAME}" \
        --query 'length(Targets)' \
        --output text
)"

if [[ "${TARGET_COUNT}" -gt 0 ]]; then
    :
else

    echo "ERROR: EventBridge rule has no targets."
    exit 1

fi

TARGET_ARN="$(
    aws events list-targets-by-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --rule "${EVENT_RULE_NAME}" \
        --query 'Targets[0].Arn' \
        --output text
)"

if [[ "${TARGET_ARN}" == "${STATE_MACHINE_ARN}" ]]; then
    :
else

    echo "ERROR: EventBridge target does not match expected state machine."
    echo "Expected: ${STATE_MACHINE_ARN}"
    echo "Actual  : ${TARGET_ARN}"
    exit 1

fi

rm -f "${LAMBDA_ZIP}" 2>/dev/null || true

echo ""
echo "============================================================"
echo "SETUP COMPLETED SUCCESSFULLY"
echo "============================================================"

echo ""
echo "AWS Account:"
echo "  ${ACCOUNT_ID}"

echo ""
echo "AWS Region:"
echo "  ${REGION}"

echo ""
echo "============================================================"
echo "ALL CHECKS PASSED"
echo "============================================================"
