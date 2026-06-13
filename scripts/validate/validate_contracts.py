"""
scripts/validate/validate_contracts.py

Validate all Gold tables in PostgreSQL against their Great Expectations data
contracts and build Data Docs — the HTML report that shows per-column
validation results (open the printed URL to screenshot it).

Usage:
    kubectl port-forward svc/fraud-postgres-postgresql -n fraud-infra 15432:5432 &
    uv run scripts/validate/validate_contracts.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

import pandas as pd

from src.pipelines.utils.db import load_pipeline_config, get_sqlalchemy_engine
from src.pipelines.governance.ge_validate import validate_table, data_docs_url
from src.pipelines.governance.suites import table_names

SAMPLE_SIZE = 10_000   # validate a random sample to keep it fast


def main() -> None:
    cfg = load_pipeline_config()
    cfg["postgres"]["host"] = "localhost"
    cfg["postgres"]["port"] = 15432

    engine = get_sqlalchemy_engine(cfg)
    schema = cfg["postgres"]["schema"]

    passed, failed, skipped = [], [], []

    for table in table_names():
        print(f"\n── Validating {schema}.{table} ──────────────────")
        try:
            df = pd.read_sql(
                f"SELECT * FROM {schema}.{table} ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}",
                engine,
            )
        except Exception as e:
            print(f"  ERROR reading table: {e}")
            failed.append(table)
            continue

        if df.empty:
            print("  SKIP: table is empty")
            skipped.append(table)
            continue

        # raise_on_critical=False → report every table, decide exit code at the end
        outcome = validate_table(table, df, build_docs=True, raise_on_critical=False)
        print(outcome.summary())
        if outcome.critical_failures:
            failed.append(table)
        else:
            passed.append(table)

    print(f"\n{'='*55}")
    print(f"Results: {len(passed)} passed, {len(failed)} failed, {len(skipped)} skipped")
    url = data_docs_url()
    if url:
        print(f"Data Docs (per-column results): {url}")
    if failed:
        print(f"Failed tables (critical violations): {failed}")
        sys.exit(1)
    print("All contracts validated — no critical violations.")


if __name__ == "__main__":
    main()
