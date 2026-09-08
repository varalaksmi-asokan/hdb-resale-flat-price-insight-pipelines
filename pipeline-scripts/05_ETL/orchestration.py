
import time
from datetime import datetime

import boto3

from config import AWS_REGION
from common import get_table_parameter, send_alert
from metadata_lambda import DEFAULT_CONTEXT_PATH, create_context, update_context
from context_tracking import compute_next_run_id, resolve_target_table_id, verify_context_update

BASE_PIPELINE_STEPS = [
    "ingestion_to_source",
    "raw_iceberg",
    "data_profiling",
    "cleaned_iceberg",
    "transformed_iceberg",
]

GLUE_JOB_NAMES = {
    "ingestion_to_source": "hdb-job-1-ingestion-to-source",
    "raw_iceberg":         "hdb-job-2-raw-iceberg",
    "data_profiling":      "hdb-job-2b-data-profiling",
    "cleaned_iceberg":     "hdb-job-3-cleaned-iceberg",
    "transformed_iceberg": "hdb-job-4-transformed-iceberg",
    "hashed_iceberg":      "hdb-job-5-hashed-iceberg",
}

GLUE_POLL_INTERVAL_SECONDS = 15
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}

ALL_STEPS = BASE_PIPELINE_STEPS + ["hashed_iceberg"]

def build_pipeline_steps(include_hashed_step: bool = True, skip_ingestion: bool = False, only_step: str = None) -> list:
    if only_step:
        if only_step not in ALL_STEPS:
            raise ValueError(f"Unknown only_step {only_step!r} - expected one of {ALL_STEPS}")
        print(f"only_step={only_step!r} - running ONLY this step, skipping the rest of the chain\n")
        return [only_step]

    steps = list(BASE_PIPELINE_STEPS)
    if include_hashed_step:
        steps.append("hashed_iceberg")
    if skip_ingestion and "ingestion_to_source" in steps:
        steps.remove("ingestion_to_source")
        print("skip_ingestion=True - skipping ingestion_to_source (source files already exist)\n")
    return steps

def run_glue_job(job_name: str, glue_client) -> dict:
    print(f"Starting Glue job: {job_name}")
    start_resp = glue_client.start_job_run(JobName=job_name)
    run_id = start_resp["JobRunId"]

    last_printed_state = None
    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
        state = status["JobRunState"]
        if state != last_printed_state:
            print(f"  [{job_name}] run_id={run_id} state={state}")
            last_printed_state = state
        if state in TERMINAL_STATES:
            return status
        time.sleep(GLUE_POLL_INTERVAL_SECONDS)

def run_local_step(step_name: str) -> dict:
    print(f"Running locally: {step_name}")
    started = time.monotonic()
    try:
        if step_name == "ingestion_to_source":
            import job_1_ingestion_to_source as job
        elif step_name == "raw_iceberg":
            import job_2_raw_iceberg as job
        elif step_name == "data_profiling":
            import job_2b_data_profiling as job
        elif step_name == "cleaned_iceberg":
            import job_3_cleaned_iceberg as job
        elif step_name == "transformed_iceberg":
            import job_4_transformed_iceberg as job
        elif step_name == "hashed_iceberg":
            import job_5_hashed_iceberg as job
        else:
            raise ValueError(f"Unknown step: {step_name}")

        result = job.main()
        duration = time.monotonic() - started
        records = len(result) if hasattr(result, "__len__") else 0
        print(f"  [{step_name}] SUCCEEDED")
        return {"JobRunState": "SUCCEEDED", "NumberOfRecords": records, "DurationSeconds": duration}

    except Exception as exc:
        duration = time.monotonic() - started
        print(f"  [{step_name}] FAILED: {exc}")
        return {"JobRunState": "FAILED", "ErrorMessage": str(exc), "NumberOfRecords": 0, "DurationSeconds": duration}

def _format_run_table(run_log: list) -> str:
    if not run_log:
        return "(no steps ran)"

    header = f"{'Step':<22} {'Status':<10} {'Records':>10} {'Duration(s)':>12}  Error"
    rule = "-" * len(header)
    lines = [header, rule]
    for entry in run_log:
        records = entry.get("records")
        records_str = f"{records:,}" if isinstance(records, int) else "-"
        duration = entry.get("duration")
        duration_str = f"{duration:.1f}" if isinstance(duration, (int, float)) else "-"
        error = entry.get("error") or "-"
        if len(error) > 60:
            error = error[:57] + "..."
        lines.append(f"{entry['step']:<22} {entry['state']:<10} {records_str:>10} {duration_str:>12}  {error}")
    return "\n".join(lines)

def run_step_or_alert(step_name: str, run_log: list, run_mode: str, glue_client=None) -> bool:
    if run_mode == "glue":
        job_name = GLUE_JOB_NAMES[step_name]
        result = run_glue_job(job_name, glue_client)
    else:
        job_name = f"local:{step_name}"
        result = run_local_step(step_name)

    state = result.get("JobRunState", "UNKNOWN")
    run_log.append({
        "step": step_name,
        "job": job_name,
        "state": state,
        "records": result.get("NumberOfRecords"),
        "duration": result.get("DurationSeconds", result.get("ExecutionTime")),
        "error": result.get("ErrorMessage"),
    })
    return state == "SUCCEEDED"

def run_pipeline(run_mode: str = "local", include_hashed_step: bool = True, skip_ingestion: bool = False,
                  track_context: bool = True, only_step: str = None):
    assert run_mode in ("local", "glue"), f"run_mode must be 'local' or 'glue', got {run_mode!r}"

    steps = build_pipeline_steps(include_hashed_step, skip_ingestion, only_step=only_step)
    glue_client = boto3.client("glue", region_name=AWS_REGION) if run_mode == "glue" else None

    run_log = []
    pipeline_succeeded = True
    failed_step = None
    pipeline_start = datetime.utcnow()

    for step in steps:
        if not run_step_or_alert(step, run_log, run_mode, glue_client):
            pipeline_succeeded = False
            failed_step = step
            break

    pipeline_end = datetime.utcnow()

    if track_context:
        layer_label = steps[0] if len(steps) == 1 else "pipeline"
        context = create_context()
        target_table_id = resolve_target_table_id(context)
        next_run_id = compute_next_run_id(context)

        status = "SUCCEEDED" if pipeline_succeeded else "FAILED"
        error_message = None
        if not pipeline_succeeded:
            failed_entry = next((e for e in run_log if e["step"] == failed_step), None)
            error_message = f"Failed at step '{failed_step}': {failed_entry.get('error') if failed_entry else 'unknown error'}"

        load_type = get_table_parameter(target_table_id, "load_type", default="FULL").strip().upper()
        watermark_updates = []
        if pipeline_succeeded and load_type != "FULL":
            watermark_updates = [{
                "table_id": target_table_id,
                "last_watermark_value": pipeline_end.isoformat(),
                "last_run_id": next_run_id,
            }]

        total_records = sum(e.get("records") or 0 for e in run_log)

        context = update_context(
            watermark_updates=watermark_updates,
            pipeline_run={
                "run_id": next_run_id,
                "table_id": target_table_id,
                "layer": layer_label,
                "start_time": pipeline_start.isoformat(),
                "end_time": pipeline_end.isoformat(),
                "status": status,
                "number_of_records": total_records,
                "error_message": error_message,
            },
            path=DEFAULT_CONTEXT_PATH,
        )
        print(f"context updated (consolidated, whole run) - run_id={next_run_id}, table_id={target_table_id}, status={status}")
        verify_context_update(target_table_id, next_run_id, watermark_updates, path=DEFAULT_CONTEXT_PATH)

    table_str = _format_run_table(run_log)
    total_records = sum(e.get("records") or 0 for e in run_log)
    subject = "HDB pipeline SUCCEEDED" if pipeline_succeeded else f"HDB pipeline FAILED at step: {failed_step}"
    message = (
        f"Mode: {run_mode}\n"
        f"Overall status: {'SUCCEEDED' if pipeline_succeeded else 'FAILED'}\n"
        f"Steps run: {len(run_log)}   Total records: {total_records:,}\n\n"
        f"{table_str}\n"
    )
    send_alert(subject=subject, message=message)

    print("\nFinal run log:")
    print(table_str)

    return pipeline_succeeded, run_log
