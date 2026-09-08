

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

export AWS_PROFILE="${AWS_PROFILE:-sujen}"
export HDB_SKIP_CONFIRM=1

ENVIRONMENTS=(dev uat prd)
PROJECT_NAME_BASE="${HDB_PROJECT_NAME_BASE:-hdb-eventdriven}"

LOG_DIR="${PROJECT_ROOT}/env_run_logs/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${LOG_DIR}"

declare -A SETUP_STATUS
declare -A PIPELINE_STATUS
declare -A TEARDOWN_STATUS

for ENV in "${ENVIRONMENTS[@]}"; do
    export PROJECT_NAME="${PROJECT_NAME_BASE}-${ENV}"
    echo ""
    echo "============================================================"
    echo "ENVIRONMENT: ${ENV}  (PROJECT_NAME=${PROJECT_NAME})"
    echo "============================================================"

    SETUP_LOG="${LOG_DIR}/${ENV}_setup.log"
    PIPELINE_LOG="${LOG_DIR}/${ENV}_pipeline.log"
    TEARDOWN_LOG="${LOG_DIR}/${ENV}_teardown.log"

    echo "[${ENV}] provisioning..."
    if bash "${SCRIPT_DIR}/setup.sh" >"${SETUP_LOG}" 2>&1; then
        SETUP_STATUS[${ENV}]="PASS"
        echo "[${ENV}] setup.sh OK - log: ${SETUP_LOG}"

        echo "[${ENV}] running pipeline against real AWS Glue jobs (HDB_RUN_MODE=glue)..."
        if HDB_RUN_MODE=glue python "${PROJECT_ROOT}/main_run_pipeline.py" >"${PIPELINE_LOG}" 2>&1; then
            PIPELINE_STATUS[${ENV}]="PASS"
            echo "[${ENV}] pipeline run OK - log: ${PIPELINE_LOG}"
        else
            PIPELINE_STATUS[${ENV}]="FAIL"
            echo "[${ENV}] pipeline run FAILED - see ${PIPELINE_LOG}"
        fi
    else
        SETUP_STATUS[${ENV}]="FAIL"
        PIPELINE_STATUS[${ENV}]="SKIPPED"
        echo "[${ENV}] setup.sh FAILED - see ${SETUP_LOG} - skipping pipeline run for this environment"
    fi

    echo "[${ENV}] tearing down..."
    if bash "${SCRIPT_DIR}/tear_down.sh" >"${TEARDOWN_LOG}" 2>&1; then
        TEARDOWN_STATUS[${ENV}]="PASS"
        echo "[${ENV}] tear_down.sh OK - log: ${TEARDOWN_LOG}"
    else
        TEARDOWN_STATUS[${ENV}]="FAIL"
        echo "[${ENV}] tear_down.sh FAILED - see ${TEARDOWN_LOG} - CHECK THE AWS CONSOLE MANUALLY FOR LEFTOVER ${ENV} RESOURCES"
    fi
done

echo ""
echo "============================================================"
echo "SUMMARY"
echo "============================================================"
printf "%-6s %-8s %-10s %-10s\n" "ENV" "SETUP" "PIPELINE" "TEARDOWN"
OVERALL_OK=1
for ENV in "${ENVIRONMENTS[@]}"; do
    printf "%-6s %-8s %-10s %-10s\n" "${ENV}" "${SETUP_STATUS[${ENV}]}" "${PIPELINE_STATUS[${ENV}]}" "${TEARDOWN_STATUS[${ENV}]}"
    if [[ "${SETUP_STATUS[${ENV}]}" != "PASS" || "${PIPELINE_STATUS[${ENV}]}" != "PASS" || "${TEARDOWN_STATUS[${ENV}]}" != "PASS" ]]; then
        OVERALL_OK=0
    fi
done
echo ""
echo "Full logs: ${LOG_DIR}"

if [[ "${OVERALL_OK}" == "1" ]]; then
    echo "ALL ENVIRONMENTS PASSED"
    exit 0
else
    echo "ONE OR MORE ENVIRONMENTS HAD A FAILURE - see logs above"
    exit 1
fi
