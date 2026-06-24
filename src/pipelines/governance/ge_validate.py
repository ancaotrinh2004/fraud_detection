"""
src/pipelines/governance/ge_validate.py

Great Expectations runner for the Gold-layer data contracts.

Validates a pandas DataFrame against the ExpectationSuite for a table
(`suites.build_suite`), persists results, and (re)builds Data Docs — the HTML
report that *shows per-column validation results* (pass / fail / observed).

Public API
──────────
  validate_table(table, df, *, build_docs=True, raise_on_critical=True)
      → ValidationOutcome

  data_docs_url()      → file:// URL of the Data Docs index (or None)

Failures are split by the `severity` meta on each expectation:
  • critical (schema / not_null / unique) → raise ValueError (blocks pipeline)
  • warn     (accepted_values / value_between) → collected, logged, non-blocking
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.pipelines.governance.suites import build_suite

logger = logging.getLogger(__name__)

# Where the GE project (suites, checkpoints, Data Docs) lives on disk.
# Override with GX_PROJECT_ROOT (e.g. a writable mount inside the Airflow image).
_REPO_ROOT = Path(__file__).parents[3]
GX_ROOT = Path(os.environ.get("GX_PROJECT_ROOT", _REPO_ROOT / "gx_project"))

_DATASOURCE = "fraud_pandas"


@dataclass
class ValidationOutcome:
    table: str
    success: bool
    n_expectations: int = 0
    n_failed: int = 0
    critical_failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    docs_url: str | None = None

    def summary(self) -> str:
        lines = [
            f"[{self.table}] {'PASS' if self.success else 'FAIL'} — "
            f"{self.n_expectations - self.n_failed}/{self.n_expectations} expectations passed"
        ]
        for c in self.critical_failures:
            lines.append(f"  CRITICAL: {c}")
        for w in self.warnings:
            lines.append(f"  WARN:     {w}")
        return "\n".join(lines)


def _get_context():
    GX_ROOT.mkdir(parents=True, exist_ok=True)
    import great_expectations as gx
    return gx.get_context(mode="file", project_root_dir=str(GX_ROOT))


def _get_or_add(adder, getter):
    """GE 1.x add/get helpers raise if the entity already exists — fall back to get."""
    try:
        return adder()
    except Exception:
        return getter()


def _failure_message(res) -> str:
    """Render an ExpectationValidationResult into a one-line, per-column message."""
    cfg = res.expectation_config
    etype = getattr(cfg, "type", "expectation")
    col = (cfg.kwargs or {}).get("column", "<table>")
    r = res.result or {}
    unexpected = r.get("unexpected_count")
    observed = r.get("observed_value")
    detail = ""
    if unexpected is not None:
        detail = f": {unexpected:,} unexpected"
    elif observed is not None:
        detail = f": observed={observed}"
    return f"{col} · {etype}{detail}"


def validate_table(
    table: str,
    df: pd.DataFrame,
    *,
    build_docs: bool = True,
    raise_on_critical: bool = True,
) -> ValidationOutcome:
    """
    Validate `df` against the GE contract for `table`.

    Returns a ValidationOutcome. If raise_on_critical and any critical-severity
    expectation fails, raises ValueError after building Data Docs (so the failing
    run is still inspectable in the report).
    """
    context = _get_context()
    import great_expectations as gx
    from great_expectations.checkpoint import UpdateDataDocsAction

    ds = _get_or_add(
        lambda: context.data_sources.add_pandas(_DATASOURCE),
        lambda: context.data_sources.get(_DATASOURCE),
    )
    asset = _get_or_add(
        lambda: ds.add_dataframe_asset(name=table),
        lambda: ds.get_asset(table),
    )
    batch_def = _get_or_add(
        lambda: asset.add_batch_definition_whole_dataframe("batch"),
        lambda: asset.get_batch_definition("batch"),
    )

    suite = build_suite(table)
    try:
        context.suites.add_or_update(suite)
    except Exception:
        _get_or_add(lambda: context.suites.add(suite),
                    lambda: context.suites.get(table))

    vdef = gx.ValidationDefinition(name=table, data=batch_def, suite=suite)
    try:
        vdef = context.validation_definitions.add_or_update(vdef)
    except Exception:
        vdef = _get_or_add(
            lambda: context.validation_definitions.add(vdef),
            lambda: context.validation_definitions.get(table),
        )

    actions = [UpdateDataDocsAction(name="update_data_docs")] if build_docs else []
    checkpoint = gx.Checkpoint(
        name=table,
        validation_definitions=[vdef],
        actions=actions,
        result_format={"result_format": "SUMMARY"},
    )
    try:
        checkpoint = context.checkpoints.add_or_update(checkpoint)
    except Exception:
        checkpoint = _get_or_add(
            lambda: context.checkpoints.add(checkpoint),
            lambda: context.checkpoints.get(table),
        )

    cp_result = checkpoint.run(batch_parameters={"dataframe": df})

    outcome = ValidationOutcome(table=table, success=bool(cp_result.success))
    for vres in cp_result.run_results.values():
        results = getattr(vres, "results", []) or []
        outcome.n_expectations += len(results)
        for res in results:
            if res.success:
                continue
            outcome.n_failed += 1
            severity = ((res.expectation_config.meta or {}).get("severity", "warn"))
            msg = _failure_message(res)
            if severity == "critical":
                outcome.critical_failures.append(msg)
            else:
                outcome.warnings.append(msg)

    outcome.docs_url = data_docs_url() if build_docs else None

    logger.info(outcome.summary())
    if outcome.docs_url:
        logger.info(f"[ge] Data Docs: {outcome.docs_url}")

    if raise_on_critical and outcome.critical_failures:
        raise ValueError(
            f"Data contract CRITICAL violations for '{table}':\n"
            + "\n".join(outcome.critical_failures)
            + (f"\nData Docs: {outcome.docs_url}" if outcome.docs_url else "")
        )

    return outcome


def data_docs_url() -> str | None:
    """file:// URL to the Data Docs index, if it has been built.

    The exact path varies slightly across GE 1.x minor versions, so glob for the
    index rather than hard-coding the full path.
    """
    candidate = GX_ROOT / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    if candidate.exists():
        return candidate.as_uri()
    for index in GX_ROOT.glob("**/data_docs/**/index.html"):
        return index.as_uri()
    return None
