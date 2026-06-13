"""
src/pipelines/utils/contract_validator.py

Backwards-compatible shim over the Great Expectations contract runner.

The real validation now lives in `src.pipelines.governance` (GE-native suites +
Data Docs). This module keeps the original `validate_contract(table, df)` entry
point so existing callers (ml/train.py, scripts/validate/validate_contracts.py)
keep working unchanged.

  • Critical violations (schema / not_null / unique) raise ValueError.
  • Non-critical violations (accepted_values / value_between) are returned.
"""

import logging

import pandas as pd

from src.pipelines.governance.ge_validate import validate_table

logger = logging.getLogger(__name__)


def validate_contract(table_name: str, df: pd.DataFrame) -> list[str]:
    """
    Validate `df` against the GE data contract for `table_name`.

    Returns the list of violation messages (warnings; criticals raise first).
    Raises ValueError if any critical-severity expectation fails.
    """
    outcome = validate_table(table_name, df, build_docs=True, raise_on_critical=True)
    return outcome.critical_failures + outcome.warnings
