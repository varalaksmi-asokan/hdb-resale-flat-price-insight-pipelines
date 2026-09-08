"""
One-off: find the WriteContext step's real Lambda output (action="update")
in a Step Functions execution history, without fighting jq/JMESPath/pager
quoting on Windows. Prints its update_errors and run_summary_table fields
directly.

Usage:
    python find_write_context_output.py <execution-arn>
"""
import json
import sys

import boto3

if len(sys.argv) != 2:
    print("Usage: python find_write_context_output.py <execution-arn>")
    sys.exit(1)

execution_arn = sys.argv[1]
sfn = boto3.client("stepfunctions", region_name="us-east-1")

paginator = sfn.get_paginator("get_execution_history")
found = 0
for page in paginator.paginate(executionArn=execution_arn, reverseOrder=False):
    for event in page.get("events", []):
        if event.get("type") != "TaskSucceeded":
            continue
        output_str = event.get("taskSucceededEventDetails", {}).get("output", "")
        if '"action":"update"' not in output_str and '"action": "update"' not in output_str:
            continue
        found += 1
        payload = json.loads(output_str).get("Payload", {})
        print(f"--- TaskSucceeded event id={event['id']} (WriteContext) ---")
        print("update_errors:", json.dumps(payload.get("update_errors"), indent=2))
        print("run_summary_table:")
        print(repr(payload.get("run_summary_table")))
        print()

if not found:
    print("No action=update Lambda output found in this execution's history.")
