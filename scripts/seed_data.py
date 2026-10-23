"""Initialize a reproducible fixture or import a real labeled CSV, without overwrites."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.data import seed_mart


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", help="Optional real labeled HW1 train.csv; names/addresses are omitted")
    parser.add_argument("--storage-dir")
    parser.add_argument("--start-month", default="2024-01")
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--rows-per-month", type=int, default=1500)
    parser.add_argument("--seed", type=int, default=2024)
    args = parser.parse_args()
    print(json.dumps(seed_mart(args.storage_dir, args.csv, args.start_month, args.months, args.rows_per_month, args.seed), indent=2))


if __name__ == "__main__":
    main()
