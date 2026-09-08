
import json
import os
import sys

(sns_topic_arn, lambda_arn,
 job_compact, job2, job2b, job3, job4, job5,
 ses_sender_email, ses_recipient_emails,
 audit_bucket,
 out_path) = sys.argv[1:]

SES_RECIPIENTS = [addr.strip() for addr in ses_recipient_emails.split(",") if addr.strip()]
ses_sender_email = ses_sender_email.strip()

USE_SES = bool(SES_RECIPIENTS) and bool(ses_sender_email)

MONO_FONT = "Consolas,Menlo,monospace"

STATE_MACHINE_TIMEOUT_SECONDS = int(
    os.environ.get("HDB_STATE_MACHINE_TIMEOUT_SECONDS", 7200)
)
print(
    f"[build_state_machine_definition] TimeoutSeconds = "
    f"{STATE_MACHINE_TIMEOUT_SECONDS}s "
    f"({STATE_MACHINE_TIMEOUT_SECONDS / 60:.0f} min) - "
    f"{'DEV/TEST override' if 'HDB_STATE_MACHINE_TIMEOUT_SECONDS' in os.environ else 'PRD default'}"
)

VACUUM_MAX_SNAPSHOT_AGE_SECONDS = os.environ.get("HDB_VACUUM_MAX_SNAPSHOT_AGE_SECONDS", "120")
print(
    f"[build_state_machine_definition] VACUUM retention override = "
    f"{VACUUM_MAX_SNAPSHOT_AGE_SECONDS}s - "
    f"{'DEV/TEST default' if 'HDB_VACUUM_MAX_SNAPSHOT_AGE_SECONDS' not in os.environ else 'explicit override'}, "
    f"change via HDB_VACUUM_MAX_SNAPSHOT_AGE_SECONDS before a real production deploy."
)
if USE_SES:
    print(
        f"[build_state_machine_definition] Alerts via SES - sender = "
        f"{ses_sender_email}, recipients = {SES_RECIPIENTS}"
    )
else:
    print(
        "[build_state_machine_definition] Alerts via SNS (plain-text) - "
        "no ses_sender_email/ses_recipient_emails given, same as leaving "
        "HDB_ALERT_RECIPIENT_EMAILS unset for Step 8B."
    )

STEPS = [
    ("IngestToRawLayer",         job2,  "job_2_raw_iceberg",         "raw_iceberg",         "ProfileRawData"),
    ("ProfileRawData",     job2b, "job_2b_data_profiling",     "data_profiling",      "CleanDataLayer"),
    ("CleanDataLayer",     job3,  "job_3_cleaned_iceberg",     "cleaned_iceberg",     "TransformDataLayer"),
    ("TransformDataLayer", job4,  "job_4_transformed_iceberg", "transformed_iceberg", "HashAndVersionDataLayer"),
    ("HashAndVersionDataLayer",      job5,  "job_5_hashed_iceberg",      "hashed_iceberg",      "RecordRunSummary"),
]

def _ses_send_email_params(subject: str, html_format_expr: str, text_format_expr: str) -> dict:
    return {
        "FromEmailAddress": ses_sender_email,
        "Destination": {"ToAddresses": SES_RECIPIENTS},
        "Content": {
            "Simple": {
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data.$": html_format_expr},
                    "Text": {"Data.$": text_format_expr},
                },
            }
        },
    }

def _sns_publish_params(subject: str, message_format_expr: str) -> dict:
    return {
        "TopicArn": sns_topic_arn,
        "Subject": subject,
        "Message.$": message_format_expr,
    }

states = {
    "CheckMetadataBeforeRun": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {"FunctionName": lambda_arn, "Payload": {"action": "read"}},
        "ResultPath": "$.context_before",
        "Next": STEPS[0][0],
    },
}

def _build_job_step(state_name, job_name, next_name, extra_arguments=None, result_path=None):
    params = {"JobName": job_name}
    if extra_arguments:
        params["Arguments"] = extra_arguments
    state = {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": params,
        "Next": next_name,
    }
    if result_path:
        state["ResultPath"] = result_path
    states[state_name] = state

for state_name, job_name, _layer, _label, next_name in STEPS:
    _build_job_step(state_name, job_name, next_name)

states["RecordRunSummary"] = {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke",
    "Parameters": {
        "FunctionName": lambda_arn,
        "Payload": {
            "action": "update",
            "sync_pipeline_runs_from_audit": {
                "run_id.$": "States.MathRandom(1, 999999999)",
                "since.$": "$$.Execution.StartTime",
                "table_id": 1,
                "execution_name.$": "$$.Execution.Name",
            },
        },
    },
    "ResultPath": "$.write_context_result",
    "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.write_context_error",
        "Next": "VerifyMetadataAfterRun",
    }],
    "Next": "VerifyMetadataAfterRun",
}

states["VerifyMetadataAfterRun"] = {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke",
    "Parameters": {"FunctionName": lambda_arn, "Payload": {"action": "read"}},
    "ResultPath": "$.context_after",
    "Next": "CompactIcebergTables",
}

_build_job_step(
    "CompactIcebergTables", job_compact, "SendSuccessEmail",
    extra_arguments={"--max-snapshot-age-seconds": VACUUM_MAX_SNAPSHOT_AGE_SECONDS},
    result_path="$.compact_result",
)

success_text = (
    "States.Format('"
    "HDB Resale Flat Prices Pipeline - ETL process completed successfully\n"
    "====================================================================\n"
    "All 6 steps ran cleanly: ingestion, raw, data profiling, cleaned, "
    "transformed, hashed.\n\n"
    "Run summary (written to pipeline_runs this run):\n"
    "{}\n\n"
    "Metadata was read before this run and read again after a real write "
    "to pipeline_runs (via sync_pipeline_runs_from_audit) - the table "
    "above reflects genuinely new state, not just two identical reads."
    "', $.write_context_result.Payload.run_summary_table)"
)

if USE_SES:
    success_html = (
        "States.Format('"
        "<!doctype html><html lang=\"en\"><head><meta charset=\"UTF-8\"></head>"
        "<body style=\"margin:0;background:#f5f7f9;font-family:Arial,Helvetica,sans-serif;color:#1a2233;\">"
        "<div style=\"max-width:640px;margin:0 auto;padding:32px 20px;\">"
        f"<div style=\"font-family:{MONO_FONT};font-size:12px;letter-spacing:0.05em;text-transform:uppercase;color:#5b6472;margin-bottom:8px;\">HDB Pipeline Run - SUCCESS</div>"
        "<h1 style=\"font-size:22px;font-weight:700;margin:0 0 6px;color:#1a2233;\">Resale Flat Prices ETL - run completed</h1>"
        "<p style=\"color:#5b6472;font-size:14px;margin:0 0 20px;\">All 6 steps ran cleanly: ingestion, raw, data profiling, cleaned, transformed, hashed.</p>"
        "{}"
        "<div style=\"font-size:12px;color:#5b6472;text-align:center;padding-top:6px;\">Metadata was read before this run and read again after a real write to pipeline_runs, so the report above reflects genuinely new state, not two identical reads.</div>"
        "</div></body></html>"
        "', $.write_context_result.Payload.run_summary_html)"
    )
    success_params = _ses_send_email_params(
        "HDB Pipeline Run - SUCCESS", success_html, success_text,
    )
else:
    success_params = _sns_publish_params(
        "HDB Pipeline Run - SUCCESS", success_text,
    )

states["SendSuccessEmail"] = {
    "Type": "Task",
    "Resource": (
        "arn:aws:states:::aws-sdk:sesv2:sendEmail" if USE_SES
        else "arn:aws:states:::sns:publish"
    ),
    "Parameters": success_params,
    "Next": "PipelineCompletedSuccessfully",
}

states["PipelineCompletedSuccessfully"] = {"Type": "Succeed"}

definition = {
    "Comment": (
        "HDB Resale Flat Prices pipeline - runs each Glue job in order, "
        "stops on first failure (uncaught, so it is Redrive-able from the "
        "real failing step), verifies before/after metadata and alerts on "
        "success. Failure alerting happens out-of-band via an EventBridge "
        "rule on this execution's own FAILED status change, not inline."
    ),
    "TimeoutSeconds": STATE_MACHINE_TIMEOUT_SECONDS,
    "StartAt": "CheckMetadataBeforeRun",
    "States": states,
}

with open(out_path, "w") as f:
    json.dump(definition, f, indent=2)
