"""Manually run the same checked temporal retraining pipeline used by Airflow."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.data import advance_month
from pipeline.registry import ModelRegistry
from pipeline.training import (
    evaluate_candidate, prepare_window, publish_candidate, train_candidate,
)


def run_retraining(storage_dir=None, run_id=None, *, advance=False, batch_size=None):
    """Keep data-quality, same-window nonregression, and atomic publication gates."""
    run_id = run_id or f"manual-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:12]}"
    supply = None
    if advance:
        supply = advance_month(f"supply-{run_id}", storage_dir=storage_dir, batch_size=batch_size)
    prepared = prepare_window(run_id=run_id, storage_dir=storage_dir)
    trained = train_candidate(prepared)
    evaluated = evaluate_candidate(trained)
    decision = publish_candidate(evaluated)
    return {
        "run_id": run_id,
        "supply": supply,
        "decision": decision,
        "active_manifest": ModelRegistry(storage_dir).current(),
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-dir")
    parser.add_argument("--run-id", help="Reuse this identifier to retry the exact same pinned run")
    parser.add_argument("--advance-month", action="store_true", help="Request one labeled month before training")
    parser.add_argument("--batch-size", type=int, help="Rows in the optional new month; default MONTHLY_BATCH_SIZE")
    args = parser.parse_args(argv)
    if args.batch_size is not None and args.batch_size < 50:
        parser.error("--batch-size must be at least 50")
    if args.batch_size is not None and not args.advance_month:
        parser.error("--batch-size requires --advance-month")
    print(json.dumps(run_retraining(
        args.storage_dir, args.run_id, advance=args.advance_month, batch_size=args.batch_size,
    ), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
