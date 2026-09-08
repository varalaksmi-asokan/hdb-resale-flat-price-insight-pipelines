
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "pipeline-scripts" / "05_ETL"))

from orchestration import run_pipeline

SKIP_INGESTION = os.environ.get("HDB_SKIP_INGESTION", "0") == "1"
ONLY_STEP = os.environ.get("HDB_ONLY_STEP") or None

RUN_MODE = os.environ.get("HDB_RUN_MODE", "glue")
if RUN_MODE not in ("local", "glue"):
    print(f"HDB_RUN_MODE={RUN_MODE!r} is not valid - expected 'local' or 'glue'", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    pipeline_succeeded, run_log = run_pipeline(run_mode=RUN_MODE, skip_ingestion=SKIP_INGESTION, only_step=ONLY_STEP)
    if not pipeline_succeeded:
        sys.exit(1)
