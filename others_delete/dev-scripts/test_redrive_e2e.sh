#!/usr/bin/env bash
# End-to-end verification of the Redrive redesign, per the "must work"
# requirement: deliberately breaks hdb-job-2-raw-iceberg's ScriptLocation
# (so its very first real Task fails for a genuine reason - a Glue Python
# Shell job whose script literally doesn't exist), starts a real execution,
# waits for it to reach a terminal state, restores the job automatically
# (via trap, so this happens even on Ctrl-C or an unexpected error), then
# queries pipeline_runs for the FAILED row the async Lambda handler should
# have written. The job is fixed again by the time you Redrive, so
# Redrive's second attempt is a genuine, real success - not staged.
#
# Run from the project root (same directory as setup.sh).
set -uo pipefail  # NOT -e: we need the trap to run even if a step fails

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"
REGION="us-east-1"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

ACCOUNT_ID="$(aws sts get-caller-identity --profile "${AWS_PROFILE}" --query Account --output text)"
BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"
AUDIT_BUCKET="${BUCKET_PREFIX}-audit-tables"
GLUE_DATABASE="${PROJECT_NAME//-/_}_database"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"
RESULTS_LOCATION="s3://${AUDIT_BUCKET}/athena-results/"

STATE_MACHINE_NAME="${PROJECT_NAME}-pipeline"
STATE_MACHINE_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
GLUE_JOB_2="hdb-job-2-raw-iceberg"

echo "=== 0) Saving hdb-job-2-raw-iceberg's current definition (so it can be restored exactly) ==="

JOB_JSON="$(aws glue get-job --profile "${AWS_PROFILE}" --region "${REGION}" --job-name "${GLUE_JOB_2}" --output json)"

ORIGINAL_ROLE="$(python -c "import json,sys; print(json.load(sys.stdin)['Job']['Role'])" <<<"${JOB_JSON}")"
ORIGINAL_SCRIPT_LOCATION="$(python -c "import json,sys; print(json.load(sys.stdin)['Job']['Command']['ScriptLocation'])" <<<"${JOB_JSON}")"
ORIGINAL_PYTHON_VERSION="$(python -c "import json,sys; print(json.load(sys.stdin)['Job']['Command'].get('PythonVersion','3.9'))" <<<"${JOB_JSON}")"
ORIGINAL_DEFAULT_ARGS="$(python -c "import json,sys; print(json.dumps(json.load(sys.stdin)['Job'].get('DefaultArguments',{})))" <<<"${JOB_JSON}")"
ORIGINAL_MAX_CAPACITY="$(python -c "import json,sys; print(json.load(sys.stdin)['Job'].get('MaxCapacity',1))" <<<"${JOB_JSON}")"
ORIGINAL_TIMEOUT="$(python -c "import json,sys; print(json.load(sys.stdin)['Job'].get('Timeout',30))" <<<"${JOB_JSON}")"

echo "Original ScriptLocation: ${ORIGINAL_SCRIPT_LOCATION}"

# Builds a Glue JobUpdate JSON for hdb-job-2-raw-iceberg with just the
# ScriptLocation swapped - every value passed as its own argv entry (not
# interpolated into the Python source itself) so nothing in
# ORIGINAL_DEFAULT_ARGS (a JSON string, which may contain quotes) can break
# the script that builds it.
build_job_update() {
    local script_location="$1"
    python - "${ORIGINAL_ROLE}" "${script_location}" "${ORIGINAL_PYTHON_VERSION}" "${ORIGINAL_DEFAULT_ARGS}" "${ORIGINAL_MAX_CAPACITY}" "${ORIGINAL_TIMEOUT}" <<'PYEOF'
import json
import sys

role, script_location, python_version, default_args_json, max_capacity, timeout = sys.argv[1:7]
print(json.dumps({
    "Role": role,
    "Command": {"Name": "pythonshell", "ScriptLocation": script_location, "PythonVersion": python_version},
    "DefaultArguments": json.loads(default_args_json),
    "MaxCapacity": float(max_capacity),
    "Timeout": int(float(timeout)),
}))
PYEOF
}

RESTORED=0
restore_job() {
    if [[ "${RESTORED}" -eq 1 ]]; then
        return
    fi
    RESTORED=1
    echo
    echo "=== Restoring hdb-job-2-raw-iceberg to its original ScriptLocation ==="
    RESTORE_UPDATE="$(build_job_update "${ORIGINAL_SCRIPT_LOCATION}")"
    aws glue update-job \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --job-name "${GLUE_JOB_2}" \
        --job-update "${RESTORE_UPDATE}" \
        >/dev/null
    echo "Restored. hdb-job-2-raw-iceberg points at ${ORIGINAL_SCRIPT_LOCATION} again."
}
trap restore_job EXIT

echo
echo "=== 1) Deliberately breaking hdb-job-2-raw-iceberg (nonexistent script) ==="

BROKEN_SCRIPT_LOCATION="${ORIGINAL_SCRIPT_LOCATION%.py}_DOES_NOT_EXIST.py"

BREAK_UPDATE="$(build_job_update "${BROKEN_SCRIPT_LOCATION}")"
aws glue update-job \
    --profile "${AWS_PROFILE}" --region "${REGION}" \
    --job-name "${GLUE_JOB_2}" \
    --job-update "${BREAK_UPDATE}" \
    >/dev/null

echo "Broken - now points at ${BROKEN_SCRIPT_LOCATION} (does not exist)."

echo
echo "=== 2) Starting a real state machine execution ==="

EXECUTION_ARN="$(
    aws stepfunctions start-execution \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --input '{}' \
        --query executionArn --output text
)"

if [[ -z "${EXECUTION_ARN}" || "${EXECUTION_ARN}" == "None" ]]; then
    echo "ERROR: start-execution did not return an executionArn - aborting (job will still be restored)."
    exit 1
fi

echo "Execution started: ${EXECUTION_ARN}"
echo "Console: https://${REGION}.console.aws.amazon.com/states/home?region=${REGION}#/v2/executions/details/${EXECUTION_ARN}"

echo
echo "=== 3) Waiting for the execution to reach a terminal state (checking every 15s) ==="

STATUS="RUNNING"
for i in $(seq 1 40); do
    STATUS="$(
        aws stepfunctions describe-execution \
            --profile "${AWS_PROFILE}" --region "${REGION}" \
            --execution-arn "${EXECUTION_ARN}" \
            --query status --output text
    )"
    echo "  [$(date +%H:%M:%S)] status = ${STATUS}"
    if [[ "${STATUS}" != "RUNNING" ]]; then
        break
    fi
    sleep 15
done

echo
if [[ "${STATUS}" == "FAILED" ]]; then
    echo "Execution FAILED as expected (this is the point of the test)."
else
    echo "WARNING: execution ended with status=${STATUS}, not FAILED as expected."
    echo "(If it's still RUNNING, the loop above just timed out - check the console link above.)"
fi

echo
echo "=== 4) Which state actually failed (should be IngestToRawLayer, NOT a Fail state) ==="

aws stepfunctions get-execution-history \
    --profile "${AWS_PROFILE}" --region "${REGION}" \
    --execution-arn "${EXECUTION_ARN}" \
    --query "events[?type=='TaskFailed' || type=='ExecutionFailed'].{type:type,id:id}" \
    --output table

echo "^ Open the console link above and use Actions -> Redrive execution to see the exact"
echo "  'State to redrive from' field - it should say IngestToRawLayer, not PipelineExecutionFailed"
echo "  or any Fail-type state (there is no Fail state in this design any more)."

echo
echo "=== 5) Giving the async EventBridge -> Lambda failure handler a few seconds to run ==="
sleep 20

echo
echo "=== 6) Querying pipeline_runs for the FAILED row the Lambda should have written ==="

QUERY_ID="$(
    aws athena start-query-execution \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --work-group "${ATHENA_WORKGROUP}" \
        --query-string "SELECT run_id, layer, status, start_time, end_time, error_message FROM \"${GLUE_DATABASE}\".\"pipeline_runs\" WHERE status = 'FAILED' ORDER BY start_time DESC LIMIT 5" \
        --result-configuration "OutputLocation=${RESULTS_LOCATION}" \
        --query QueryExecutionId --output text
)"

for i in $(seq 1 15); do
    QSTATE="$(
        aws athena get-query-execution \
            --profile "${AWS_PROFILE}" --region "${REGION}" \
            --query-execution-id "${QUERY_ID}" \
            --query QueryExecution.Status.State --output text
    )"
    if [[ "${QSTATE}" == "SUCCEEDED" || "${QSTATE}" == "FAILED" || "${QSTATE}" == "CANCELLED" ]]; then
        break
    fi
    sleep 2
done

if [[ "${QSTATE}" == "SUCCEEDED" ]]; then
    aws athena get-query-results \
        --profile "${AWS_PROFILE}" --region "${REGION}" \
        --query-execution-id "${QUERY_ID}" \
        --query "ResultSet.Rows[].Data[].VarCharValue" \
        --output table
else
    echo "Athena query did not succeed (state=${QSTATE}) - check manually."
fi

echo
echo "Also check your configured alert inbox for the FAILURE alert email."
echo
echo "=== 7) Job is being restored now (see trap output below) ==="
echo "Once restored, redrive the SAME execution to confirm it resumes from"
echo "IngestToRawLayer and completes for real:"
echo
echo "  aws stepfunctions redrive-execution --profile ${AWS_PROFILE} --region ${REGION} --execution-arn \"${EXECUTION_ARN}\""
echo
echo "Then poll it the same way:"
echo "  aws stepfunctions describe-execution --profile ${AWS_PROFILE} --region ${REGION} --execution-arn \"${EXECUTION_ARN}\" --query status --output text"
