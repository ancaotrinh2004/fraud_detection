"""
Export drift_validation_report.csv from agg_feature_health_daily.

Prerequisites:
  kubectl port-forward -n fraud-infra svc/fraud-postgres-postgresql 15432:5432

Run:
  python scripts/generate_drift_report.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1]))

import pandas as pd
from sqlalchemy import create_engine

SCHEMA = "gold_fraud"
OUTPUT = Path("data/drift_validation_report.csv")

PG_URL = "postgresql://fraud_user:fraud_pass@localhost:5432/fraud_detection"


def main():
    engine = create_engine(PG_URL)

    df = pd.read_sql(
        f"""
        SELECT
            monitoring_date     AS date,
            feature_name,
            mean_value,
            stddev_value,
            psi_vs_baseline,
            CASE
                WHEN psi_vs_baseline = 0           THEN 'baseline'
                WHEN psi_vs_baseline < 0.005       THEN 'stable'
                WHEN psi_vs_baseline < 0.025       THEN 'ramp_up'
                ELSE                                    'detected'
            END AS drift_status
        FROM {SCHEMA}.agg_feature_health_daily
        ORDER BY monitoring_date
        """,
        engine,
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT, index=False)
    print(f"✓ Saved {len(df)} rows → {OUTPUT.resolve()}")
    print()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
