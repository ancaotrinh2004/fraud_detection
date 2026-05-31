"""src/pipelines/utils/quality.py — Reusable quality checks for all pipeline layers."""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    pipeline: str
    checks: list = field(default_factory=list)

    def add(self, check: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        self.checks.append({"check": check, "status": status, "detail": detail})
        if not passed:
            logger.warning(f"[{self.pipeline}] Quality FAIL — {check}: {detail}")
        else:
            logger.info(f"[{self.pipeline}] Quality PASS — {check}")

    @property
    def passed(self) -> bool:
        return all(c["status"] == "PASS" for c in self.checks)

    def summary(self) -> str:
        lines = [f"Quality report for [{self.pipeline}]:"]
        for c in self.checks:
            lines.append(f"  {c['status']:4s}  {c['check']}: {c['detail']}")
        return "\n".join(lines)


# ── Individual check functions ────────────────────────────────────────────────

def check_not_empty(df: pd.DataFrame, qr: QualityResult) -> None:
    qr.add("not_empty", len(df) > 0, f"{len(df)} rows")


def check_no_nulls(df: pd.DataFrame, cols: list[str], qr: QualityResult) -> None:
    for col in cols:
        if col not in df.columns:
            qr.add(f"no_nulls.{col}", False, "column missing")
            continue
        n_null = df[col].isna().sum()
        qr.add(f"no_nulls.{col}", n_null == 0, f"{n_null} nulls")


def check_unique(df: pd.DataFrame, cols: list[str], qr: QualityResult) -> None:
    n_dupes = df.duplicated(subset=cols).sum()
    qr.add(f"unique({','.join(cols)})", n_dupes == 0, f"{n_dupes} duplicates")


def check_schema(df: pd.DataFrame, expected_cols: list[str], qr: QualityResult) -> None:
    missing = [c for c in expected_cols if c not in df.columns]
    qr.add("schema_columns", len(missing) == 0,
           f"missing: {missing}" if missing else "all columns present")


def check_fraud_rate(df: pd.DataFrame, col: str,
                     min_rate: float, max_rate: float,
                     qr: QualityResult) -> None:
    if col not in df.columns or len(df) == 0:
        return
    rate = df[col].mean()
    qr.add("fraud_rate",
           min_rate <= rate <= max_rate,
           f"{rate:.4f} (expected {min_rate}–{max_rate})")


def check_volume(current_rows: int, baseline_rows: int,
                 drop_threshold: float, qr: QualityResult) -> None:
    if baseline_rows == 0:
        return
    drop = 1 - (current_rows / baseline_rows)
    qr.add("volume_drop",
           drop <= drop_threshold,
           f"current={current_rows}, baseline={baseline_rows}, drop={drop:.1%}")


def check_referential(df: pd.DataFrame, fk_col: str,
                       ref_values: set, qr: QualityResult,
                       dead_letter_path: Optional[str] = None) -> pd.DataFrame:
    """Returns the valid subset; optionally writes unmatched to dead-letter."""
    unmatched = df[~df[fk_col].isin(ref_values)]
    matched = df[df[fk_col].isin(ref_values)]
    qr.add(f"referential.{fk_col}",
           len(unmatched) == 0,
           f"{len(unmatched)} unmatched rows")
    if len(unmatched) > 0 and dead_letter_path:
        import os; os.makedirs(dead_letter_path, exist_ok=True)
        from datetime import datetime
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = f"{dead_letter_path}/unmatched_{fk_col}_{ts}.parquet"
        unmatched.to_parquet(path, index=False)
        logger.warning(f"Dead-letter: {len(unmatched)} rows → {path}")
    return matched