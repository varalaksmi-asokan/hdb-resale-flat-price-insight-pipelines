
import datetime

from common import get_table_parameter
from metadata_lambda import DEFAULT_CONTEXT_PATH, create_context, update_context

CONTEXT_DIR = DEFAULT_CONTEXT_PATH.parent

def _context_path_for_layer(layer: str):
    return CONTEXT_DIR / f"context_{layer}.json"

def resolve_target_table_id(context: dict, table_name: str = "resaleflat_price") -> int:
    metadata_rows = context["tables"].get("metadata_tables", [])
    target_row = next((r for r in metadata_rows if r.get("table_name") == table_name), None)
    return int(target_row["table_id"]) if target_row else 1

def compute_next_run_id(context: dict) -> int:
    existing_run_ids = [
        int(r["run_id"]) for r in context["tables"].get("pipeline_runs", [])
        if r.get("run_id") is not None
    ]
    return max(existing_run_ids, default=0) + 1

def run_step_with_context(job_main, layer: str, table_name: str = "resaleflat_price") -> dict:
    context_path = _context_path_for_layer(layer)
    context = create_context(path=context_path)

    target_table_id = resolve_target_table_id(context, table_name)
    next_run_id = compute_next_run_id(context)

    start_time = datetime.datetime.utcnow()
    try:
        result = job_main()
        end_time = datetime.datetime.utcnow()
        status = "SUCCEEDED"
        error_message = None
        number_of_records = len(result) if hasattr(result, "__len__") else 0
        print(f"{layer} SUCCEEDED" + (f" - {number_of_records} record(s)" if hasattr(result, "__len__") else ""))
    except Exception as exc:
        end_time = datetime.datetime.utcnow()
        status = "FAILED"
        error_message = str(exc)
        number_of_records = 0
        print(f"{layer} FAILED: {exc}")

    duration_seconds = (end_time - start_time).total_seconds()

    load_type = get_table_parameter(target_table_id, "load_type", default="FULL").strip().upper()
    watermark_updates = []
    if status == "SUCCEEDED" and load_type != "FULL":
        watermark_updates = [{
            "table_id": target_table_id,
            "last_watermark_value": end_time.isoformat(),
            "last_run_id": next_run_id,
        }]

    context = update_context(
        watermark_updates=watermark_updates,
        pipeline_run={
            "run_id": next_run_id,
            "table_id": target_table_id,
            "layer": layer,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": status,
            "number_of_records": number_of_records,
            "error_message": error_message,
        },
        path=context_path,
    )
    print(f"context updated - run_id={next_run_id}, table_id={target_table_id}, status={status}")

    verify_context_update(target_table_id, next_run_id, watermark_updates, path=context_path)

    if status == "FAILED":
        exc_to_raise = RuntimeError(error_message)
        exc_to_raise.number_of_records = number_of_records
        exc_to_raise.duration_seconds = duration_seconds
        raise exc_to_raise

    return {
        "status": status,
        "context": context,
        "target_table_id": target_table_id,
        "run_id": next_run_id,
        "watermark_updates": watermark_updates,
        "error_message": error_message,
        "number_of_records": number_of_records,
        "duration_seconds": duration_seconds,
    }

def get_previous_stage_count(layer: str, table_id: int, context: dict = None) -> int:
    if context is None:
        context = create_context()
    candidates = [
        r for r in context["tables"].get("pipeline_runs", [])
        if r.get("layer") == layer
        and str(r.get("table_id")) == str(table_id)
        and r.get("status") == "SUCCEEDED"
        and r.get("number_of_records") is not None
    ]
    if not candidates:
        return None
    latest = max(candidates, key=lambda r: int(r["run_id"]))
    return int(latest["number_of_records"])

def reconcile_count(my_count: int, expected_count, layer: str, previous_layer: str, my_rejected_count: int = 0) -> tuple:
    accounted_for = my_count + my_rejected_count

    if expected_count is None:
        return "SUCCEEDED", (
            f"{layer}: wrote {my_count} row(s)"
            + (f", rejected {my_rejected_count}" if my_rejected_count else "")
            + f" - no prior SUCCEEDED {previous_layer!r} run to reconcile against"
        )

    if accounted_for == expected_count:
        return "SUCCEEDED", (
            f"{layer}: {my_count} kept"
            + (f" + {my_rejected_count} rejected" if my_rejected_count else "")
            + f" = {accounted_for}, matches {previous_layer}'s recorded {expected_count}"
        )

    gap = expected_count - accounted_for
    return "MISMATCH", (
        f"{layer}: {my_count} kept"
        + (f" + {my_rejected_count} rejected" if my_rejected_count else "")
        + f" = {accounted_for}, expected {expected_count} from {previous_layer} "
        f"- {gap} row(s) unaccounted for"
    )

def record_stage_result(layer: str, table_id: int, status: str, my_count: int, reason: str,
                         start_time: datetime.datetime, context: dict = None) -> dict:
    if context is None:
        context = create_context()
    run_id = compute_next_run_id(context)
    end_time = datetime.datetime.utcnow()
    updated = update_context(
        pipeline_run={
            "run_id": run_id,
            "table_id": table_id,
            "layer": layer,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": status,
            "number_of_records": my_count,
            "error_message": reason,
        },
    )
    print(f"{layer}: recorded pipeline_runs run_id={run_id} status={status} - {reason}")
    return updated

def verify_context_update(target_table_id: int, run_id: int, watermark_updates: list, path=DEFAULT_CONTEXT_PATH) -> bool:
    fresh = create_context(path=path)

    watermark_row = next(
        (w for w in fresh["tables"].get("table_watermarks", []) if str(w.get("table_id")) == str(target_table_id)),
        None,
    )
    run_row = next(
        (r for r in fresh["tables"].get("pipeline_runs", []) if str(r.get("run_id")) == str(run_id)),
        None,
    )

    assert run_row is not None, f"pipeline_runs has no row for run_id={run_id}"
    print(f"OK  pipeline_runs contains run_id={run_id} (status={run_row.get('status')})")

    if watermark_updates:
        expected = watermark_updates[0]["last_watermark_value"]
        assert watermark_row is not None, f"table_watermarks has no row for table_id={target_table_id}"
        assert watermark_row.get("last_watermark_value") == expected, (
            f"table_watermarks[table_id={target_table_id}] did not reflect the new watermark - "
            f"got {watermark_row.get('last_watermark_value')!r}, expected {expected!r}"
        )
        print(f"OK  table_watermarks[table_id={target_table_id}] advanced to {watermark_row['last_watermark_value']}")
    else:
        print("Watermark intentionally left unchanged (step failed, or this table's load_type is FULL - "
              "full-load tables truncate and reload every run, so there's no meaningful watermark to advance).")

    print("CONTEXT UPDATE TEST PASSED")
    return True
