"""src/pipelines/utils/db.py — DB connection helpers."""

import os
import yaml
from pathlib import Path
from contextlib import contextmanager

import duckdb
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text


_CONFIG_PATH = Path(__file__).parents[3] / "configs" / "pipeline_config.yaml"


def load_pipeline_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_pg_connection_string(cfg: dict | None = None) -> str:
    if cfg is None:
        cfg = load_pipeline_config()
    pg = cfg["postgres"]
    password = os.environ.get("POSTGRES_PASSWORD", pg["password"])
    return (
        f"postgresql://{pg['user']}:{password}"
        f"@{pg['host']}:{pg['port']}/{pg['database']}"
    )


def get_sqlalchemy_engine(cfg: dict | None = None):
    return create_engine(
        get_pg_connection_string(cfg),
        pool_pre_ping=True,
        connect_args={"connect_timeout": 30},
    )


@contextmanager
def pg_cursor(cfg: dict | None = None):
    """Yield a psycopg2 cursor; auto-commit or rollback."""
    conn_str = get_pg_connection_string(cfg)
    conn = psycopg2.connect(conn_str)
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema(cfg: dict | None = None) -> None:
    """Create gold_fraud schema and pipeline_run_log if not exists."""
    if cfg is None:
        cfg = load_pipeline_config()
    schema = cfg["postgres"]["schema"]
    with pg_cursor(cfg) as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_run_log (
                run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                pipeline_name   TEXT NOT NULL,
                start_ts        TIMESTAMP NOT NULL,
                end_ts          TIMESTAMP,
                status          TEXT,
                input_rows      BIGINT,
                output_rows     BIGINT,
                error_summary   TEXT
            );
        """)


def log_run(pipeline_name: str, start_ts, end_ts, status: str,
            input_rows: int, output_rows: int, error_summary: str = None,
            cfg: dict | None = None) -> None:
    if cfg is None:
        cfg = load_pipeline_config()
    schema = cfg["postgres"]["schema"]
    with pg_cursor(cfg) as cur:
        cur.execute(f"""
            INSERT INTO {schema}.pipeline_run_log
                (pipeline_name, start_ts, end_ts, status, input_rows, output_rows, error_summary)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (pipeline_name, start_ts, end_ts, status, input_rows, output_rows, error_summary))