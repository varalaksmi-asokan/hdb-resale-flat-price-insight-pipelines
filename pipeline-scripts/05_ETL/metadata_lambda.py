
import json
from pathlib import Path

import boto3

from config import AWS_REGION, PROJECT_NAME

LAMBDA_FUNCTION_NAME = f"mission-{PROJECT_NAME}-metadata-reader"

DEFAULT_CONTEXT_PATH = Path(__file__).resolve().parent.parent / "infrastructure_files" / "04_json_scripts" / "context.json"

_lambda_client = boto3.client("lambda", region_name=AWS_REGION)

def call_metadata_lambda(action: str = "read", **payload) -> dict:
    response = _lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({"action": action, **payload}).encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    if response.get("FunctionError"):
        raise RuntimeError(f"metadata Lambda failed: {result}")
    return result

def _save_context(ctx: dict, path: Path = DEFAULT_CONTEXT_PATH) -> str:
    context_json = json.dumps(ctx, indent=2, default=str)
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(context_json)
    except OSError as exc:
        print(f"WARNING - could not write local debug context cache to {path}: {exc} (non-fatal, continuing)")
    return context_json

def load_context(path: Path = DEFAULT_CONTEXT_PATH) -> dict:
    return json.loads(Path(path).read_text())

def create_context(path: Path = DEFAULT_CONTEXT_PATH) -> dict:
    ctx = call_metadata_lambda("read")
    if ctx.get("errors"):
        print("WARNING - some metadata tables failed to read:", ctx["errors"])
    for table_name, rows in ctx["tables"].items():
        print(f"  {table_name:20s} {len(rows)} row(s)")
    _save_context(ctx, path)
    print(f"Context saved to {path}")
    return ctx

def update_context(watermark_updates: list = None, pipeline_run: dict = None, path: Path = DEFAULT_CONTEXT_PATH) -> dict:
    updated = call_metadata_lambda(
        "update",
        watermark_updates=watermark_updates or [],
        pipeline_run=pipeline_run,
    )
    if updated.get("update_errors"):
        raise RuntimeError(f"metadata update failed: {updated['update_errors']}")
    _save_context(updated, path)
    print(f"Context saved to {path}")
    return updated
