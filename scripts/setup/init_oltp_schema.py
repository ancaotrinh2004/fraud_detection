"""
Initialize the OLTP source schema (CDC origin for the streaming track).

Creates `oltp.transaction_events` and the Debezium logical-replication
publication. Run once after Postgres is up (with wal_level=logical) and before
starting the generator's OLTP writer / the Debezium connector.

Usage:
  python scripts/setup/init_oltp_schema.py            # create-if-not-exists
  python scripts/setup/init_oltp_schema.py --drop     # drop + recreate (destructive)
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from src.pipelines.utils.db import load_pipeline_config
from src.pipelines.oltp.ddl import create_oltp_table, drop_oltp_table


def main():
    parser = argparse.ArgumentParser(description="Init OLTP source schema for CDC")
    parser.add_argument("--drop", action="store_true",
                        help="Drop the OLTP table + publication first (destructive)")
    args = parser.parse_args()

    cfg = load_pipeline_config()
    if args.drop:
        print("⚠ Dropping existing OLTP table + publication...")
        drop_oltp_table(cfg)
    create_oltp_table(cfg)
    print("✓ OLTP schema ready (oltp.transaction_events + CDC publication).")


if __name__ == "__main__":
    main()
