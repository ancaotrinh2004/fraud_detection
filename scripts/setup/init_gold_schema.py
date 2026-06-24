"""
Initialize Gold PostgreSQL schema and tables.

By default this DROPS the entire gold_fraud schema (all existing tables/partitions)
and recreates it from scratch. Pass --no-drop to only create missing tables.

Run from project root:
  kubectl port-forward -n fraud-infra svc/fraud-postgres-postgresql 5432:5432
  python scripts/setup/init_gold_schema.py            # drop + recreate (destructive)
  python scripts/setup/init_gold_schema.py --no-drop  # create-if-not-exists only
"""

import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[2]))

from src.pipelines.utils.db import load_pipeline_config
from src.pipelines.gold.ddl import create_all_tables, drop_all_tables


def main():
    parser = argparse.ArgumentParser(description="Init Gold schema")
    parser.add_argument("--no-drop", action="store_true",
                        help="Skip dropping the schema (create-if-not-exists only)")
    args = parser.parse_args()

    cfg = load_pipeline_config()
    # Override for local port-forward — pipeline_config.yaml targets K8s internal hostname
    cfg["postgres"]["host"] = "localhost"
    cfg["postgres"]["port"] = 5432
    print(f"Connecting to PostgreSQL: {cfg['postgres']['host']}:{cfg['postgres']['port']}")

    if not args.no_drop:
        print("⚠ Dropping existing Gold schema (destructive)...")
        drop_all_tables(cfg)

    create_all_tables(cfg)


if __name__ == "__main__":
    main()
