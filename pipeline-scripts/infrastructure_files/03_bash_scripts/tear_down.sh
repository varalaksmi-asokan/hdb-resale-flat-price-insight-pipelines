

set -Eeuo pipefail

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"

REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

ACCOUNT_ID="$(
    aws sts get-caller-identity \
        --profile "${AWS_PROFILE}" \
        --query Account \
        --output text \
        --region "${REGION}" \
        --no-cli-pager
)"

BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"

BUCKETS=(
    "${BUCKET_PREFIX}-source"
    "${BUCKET_PREFIX}-raw"
    "${BUCKET_PREFIX}-cleaned"
    "${BUCKET_PREFIX}-transformed"
    "${BUCKET_PREFIX}-hashed"
    "${BUCKET_PREFIX}-failed"
    "${BUCKET_PREFIX}-pipeline-scripts"
    "${BUCKET_PREFIX}-audit-tables"
    "aws-glue-assets-${ACCOUNT_ID}-${REGION}"
)

GLUE_DATABASE="${PROJECT_NAME//-/_}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"

GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"

EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"

STEPFUNCTIONS_ROLE_NAME="${PROJECT_NAME}-stepfunctions-role"

STATE_MACHINE_NAME="${PROJECT_NAME}-pipeline"

GLUE_JOB_1="hdb-job-1-ingestion-to-source"
GLUE_JOB_2="hdb-job-2-raw-iceberg"
GLUE_JOB_2B="hdb-job-2b-data-profiling"
GLUE_JOB_3="hdb-job-3-cleaned-iceberg"
GLUE_JOB_4="hdb-job-4-transformed-iceberg"
GLUE_JOB_5="hdb-job-5-hashed-iceberg"

GHA_ROLE_NAME="${GITHUB_ACTIONS_ROLE_NAME:-hdb-pipeline-github-actions}"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

INGESTION_TRIGGER_RULE_NAME="${PROJECT_NAME}-ingestion-complete-trigger"

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

echo ""
echo "============================================================"
echo "HDB PIPELINE TEARDOWN"
echo "============================================================"
echo ""

echo "AWS Account : ${ACCOUNT_ID}"
echo "AWS Profile : ${AWS_PROFILE}"
echo "AWS Region  : ${REGION}"
echo "PROJECT_NAME: ${PROJECT_NAME}"
echo ""

if [[ "${HDB_SKIP_CONFIRM:-0}" == "1" ]]; then
    echo "Are you sure? (yes/no): yes  (auto-confirmed - HDB_SKIP_CONFIRM=1)"
else
    read -r -p "Are you sure? (yes/no): " CONFIRM

    if [[ "${CONFIRM}" != "yes" ]]; then
        echo ""
        echo "Teardown cancelled."
        exit 0
    fi
fi

echo "Deleting resources..."

STATE_MACHINE_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"

if aws stepfunctions describe-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws stepfunctions delete-state-machine \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --no-cli-pager \
        >/dev/null

fi

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --name "${EVENT_RULE_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    TARGET_IDS="$(
        aws events list-targets-by-rule \
            --profile "${AWS_PROFILE}" \
            --rule "${EVENT_RULE_NAME}" \
            --region "${REGION}" \
            --query 'Targets[].Id' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${TARGET_IDS}" && "${TARGET_IDS}" != "None" ]]; then
        aws events remove-targets \
            --profile "${AWS_PROFILE}" \
            --rule "${EVENT_RULE_NAME}" \
            --ids ${TARGET_IDS} \
            --region "${REGION}" \
            --no-cli-pager \
            >/dev/null
    fi

    aws events delete-rule \
        --profile "${AWS_PROFILE}" \
        --name "${EVENT_RULE_NAME}" \
        --region "${REGION}" \
        --no-cli-pager \
        >/dev/null

fi

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --name "${INGESTION_TRIGGER_RULE_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    TARGET_IDS="$(
        aws events list-targets-by-rule \
            --profile "${AWS_PROFILE}" \
            --rule "${INGESTION_TRIGGER_RULE_NAME}" \
            --region "${REGION}" \
            --query 'Targets[].Id' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${TARGET_IDS}" && "${TARGET_IDS}" != "None" ]]; then
        aws events remove-targets \
            --profile "${AWS_PROFILE}" \
            --rule "${INGESTION_TRIGGER_RULE_NAME}" \
            --ids ${TARGET_IDS} \
            --region "${REGION}" \
            --no-cli-pager \
            >/dev/null
    fi

    aws events delete-rule \
        --profile "${AWS_PROFILE}" \
        --name "${INGESTION_TRIGGER_RULE_NAME}" \
        --region "${REGION}" \
        --no-cli-pager \
        >/dev/null

fi

SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${SNS_TOPIC_NAME}"
echo "Skipping SNS topic deletion (preserved by design - see STEP 2 comment): ${SNS_TOPIC_ARN}"

if aws lambda get-function \
    --profile "${AWS_PROFILE}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws lambda delete-function \
        --profile "${AWS_PROFILE}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --region "${REGION}" \
        --no-cli-pager \
        >/dev/null

fi

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    REMAINING_TABLES="$(
        aws glue get-tables \
            --profile "${AWS_PROFILE}" \
            --database-name "${GLUE_DATABASE}" \
            --region "${REGION}" \
            --query 'TableList[].Name' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${REMAINING_TABLES}" && "${REMAINING_TABLES}" != "None" ]]; then
        for TABLE_NAME in ${REMAINING_TABLES}; do
            aws glue delete-table \
                --profile "${AWS_PROFILE}" \
                --database-name "${GLUE_DATABASE}" \
                --name "${TABLE_NAME}" \
                --region "${REGION}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

fi

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws glue delete-database \
        --profile "${AWS_PROFILE}" \
        --name "${GLUE_DATABASE}" \
        --region "${REGION}" \
        --no-cli-pager \
        >/dev/null

    if aws glue get-database \
        --profile "${AWS_PROFILE}" \
        --name "${GLUE_DATABASE}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then
        echo ""
        echo "ERROR: Glue database ${GLUE_DATABASE} still exists after delete-database."
        echo "Check IAM/Lake Formation permissions for ${AWS_PROFILE}, or delete it"
        echo "manually: aws glue delete-database --name ${GLUE_DATABASE} --region ${REGION}"
        exit 1
    fi

fi

for JOB_NAME in "${GLUE_JOB_1}" "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}"; do

    if aws glue get-job \
        --profile "${AWS_PROFILE}" \
        --job-name "${JOB_NAME}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        aws glue delete-job \
            --profile "${AWS_PROFILE}" \
            --job-name "${JOB_NAME}" \
            --region "${REGION}" \
            --no-cli-pager \
            >/dev/null

    fi

done

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${LAMBDA_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${LAMBDA_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GLUE_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GLUE_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GLUE_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GLUE_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${EVENT_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${EVENT_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${EVENT_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${EVENT_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GHA_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GHA_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GHA_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GHA_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

for BUCKET in "${BUCKETS[@]}"; do

    if aws s3api head-bucket \
        --profile "${AWS_PROFILE}" \
        --bucket "${BUCKET}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        aws s3 rm \
            --profile "${AWS_PROFILE}" \
            "s3://${BUCKET}" \
            --recursive \
            --region "${REGION}" \
            --no-cli-pager \
            --quiet

        aws s3api delete-bucket \
            --profile "${AWS_PROFILE}" \
            --bucket "${BUCKET}" \
            --region "${REGION}" \
            --no-cli-pager

    fi

done

echo "============================================================"
echo "HDB PIPELINE TEARDOWN COMPLETED"
echo "============================================================"
echo ""

echo "Deleted/checked resources:"
echo "  Step Functions State Machine"
echo "  S3 Buckets"
echo "  Glue Tables"
echo "  Glue Database"
echo "  Glue Jobs"
echo "  Lambda Function"
echo "  Lambda IAM Role"
echo "  Glue IAM Role"
echo "  EventBridge IAM Role"
echo "  Step Functions IAM Role"
echo "  GitHub Actions IAM Role"
echo "  SNS Topic (preserved - subscriptions kept, not deleted)"
echo "  EventBridge Rule"
echo "  Ingestion-Complete Trigger Rule"

echo ""
echo "AWS Account : ${ACCOUNT_ID}"
echo "AWS Region  : ${REGION}"
echo ""

echo "============================================================"
