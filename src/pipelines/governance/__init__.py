"""
src/pipelines/governance/

Data governance built on Great Expectations (GE-native).

The ExpectationSuites in `suites.py` are the single source of truth for the
Gold-layer data contracts — per-column validation rules AND descriptions.

  suites.py      → GE-native ExpectationSuite definitions for all Gold tables.
  ge_validate.py → runner: validate a DataFrame, build Data Docs (HTML), and
                   either return results or raise on critical failures.

Two governance surfaces are driven from these suites:
  • GE Data Docs — per-column validation RESULTS (pass/fail/observed) as HTML.
  • DataHub       — catalog: table/column descriptions + contract + assertions
                    (scripts/governance/emit_contracts.py reads from here).
"""
