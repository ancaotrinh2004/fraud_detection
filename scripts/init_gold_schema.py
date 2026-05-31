"""
Initialize Gold PostgreSQL schema and tables.
Run from project root: python scripts/init_gold_schema.py

Requires kubectl port-forward active:
  kubectl port-forward -n fraud-infra svc/fraud-postgres-postgresql 5432:5432
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.pipelines.utils.db import load_pipeline_config
from src.pipelines.gold.ddl import create_all_tables


def main():
    cfg = load_pipeline_config()
    # Override for local port-forward — pipeline_config.yaml targets K8s internal hostname
    cfg["postgres"]["host"] = "localhost"
    cfg["postgres"]["port"] = 5432
    print(f"Connecting to PostgreSQL: {cfg['postgres']['host']}:{cfg['postgres']['port']}")
    create_all_tables(cfg)


if __name__ == "__main__":
    main()
