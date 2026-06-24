"""
scripts/governance/emit_contracts.py

Emit data governance metadata to DataHub for all Gold tables:
  - Dataset description (table-level)
  - Schema metadata (column name, type, description, nullable)
  - Column-level assertions (not_null, accepted_values, value_between)
  - DataContract linking all assertions

Reads: contracts/*.yaml
Pushes: DataHub GMS at http://localhost:8080

Usage:
    kubectl port-forward svc/datahub-datahub-gms 8080:8080 -n datahub &
    python scripts/governance/emit_contracts.py
"""

import re
import sys
from pathlib import Path
from typing import Union

sys.path.insert(0, str(Path(__file__).parents[2]))

from src.pipelines.governance.suites import TABLE_SPECS, TableSpec

from datahub.emitter.mce_builder import make_dataset_urn, make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParameterTypeClass,
    AssertionStdParametersClass,
    AssertionTypeClass,
    BooleanTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)

GMS_URL       = "http://localhost:8080"
ENV           = "DEV"
PG_DB         = "fraud_detection"
PG_SCHEMA     = "gold_fraud"


def _spec_to_contract(spec: TableSpec) -> dict:
    """Adapt a GE-native TableSpec to the dict shape the emitters consume."""
    columns: dict[str, dict] = {}
    for col in spec.columns:
        tests: list = []
        if col.not_null:
            tests.append("not_null")
        if col.unique:
            tests.append("unique")
        if col.accepted_values is not None:
            tests.append(f"accepted_values({col.accepted_values})")
        if col.value_between is not None:
            lo, hi = col.value_between
            tests.append(f"value_between({lo}, {hi})")
        columns[col.name] = {
            "type": col.type,
            "nullable": col.nullable,
            "description": col.description,
            "tests": tests,
        }
    return {
        "table": spec.table,
        "schema": spec.schema,
        "description": spec.description,
        "columns": columns,
    }

FieldType = Union[
    BooleanTypeClass, DateTypeClass, NumberTypeClass,
    StringTypeClass, TimeTypeClass,
]

_TYPE_MAP: dict[str, FieldType] = {
    "bigint": NumberTypeClass(),   "int": NumberTypeClass(),
    "integer": NumberTypeClass(),  "numeric": NumberTypeClass(),
    "float": NumberTypeClass(),    "double": NumberTypeClass(),
    "smallint": NumberTypeClass(), "varchar": StringTypeClass(),
    "text": StringTypeClass(),     "char": StringTypeClass(),
    "boolean": BooleanTypeClass(), "bool": BooleanTypeClass(),
    "timestamp": TimeTypeClass(),  "date": DateTypeClass(),
}


def _urn(table: str) -> str:
    return make_dataset_urn("postgres", f"{PG_DB}.{PG_SCHEMA}.{table}", ENV)


def _field_type(col_type: str) -> SchemaFieldDataTypeClass:
    base: FieldType = _TYPE_MAP.get(col_type.lower().split("(")[0].strip(), StringTypeClass())
    return SchemaFieldDataTypeClass(type=base)


# ── Emitters ──────────────────────────────────────────────────────────────────

def emit_properties(e: DatahubRestEmitter, urn: str, c: dict) -> None:
    e.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(
            name=c["table"],
            description=c.get("description", "").strip(),
            customProperties={"schema": c.get("schema", PG_SCHEMA)},
        ),
    ))


def emit_schema(e: DatahubRestEmitter, urn: str, c: dict) -> None:
    fields = [
        SchemaFieldClass(
            fieldPath=col,
            type=_field_type(defn.get("type", "varchar")),
            nativeDataType=defn.get("type", "varchar"),
            nullable=defn.get("nullable", True),
            description=defn.get("description", ""),
        )
        for col, defn in c.get("columns", {}).items()
    ]
    e.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=SchemaMetadataClass(
            schemaName=f"{PG_SCHEMA}.{c['table']}",
            platform="urn:li:dataPlatform:postgres",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        ),
    ))


def _parse_tests(raw: list) -> list[dict]:
    out = []
    for t in raw:
        t = str(t).strip()
        if t == "not_null":
            out.append({"kind": "not_null"})
        elif t == "unique":
            pass  # no UNIQUE operator in DataHub SDK — tracked via not_null
        elif t.startswith("accepted_values("):
            out.append({"kind": "accepted_values",
                        "values": re.findall(r"'([^']+)'", t)})
        elif t.startswith("value_between("):
            m = re.match(r"value_between\(([^,]+),\s*([^)]+)\)", t)
            if m:
                out.append({"kind": "value_between",
                            "min": float(m.group(1)),
                            "max": float(m.group(2))})
    return out


def _build_assertion(
    table_urn: str, col: str, kind: str, **kw
) -> tuple[str, MetadataChangeProposalWrapper] | tuple[None, None]:
    uid  = f"{table_urn.split(',')[-1].rstrip(')')}_{col}_{kind}"
    aurn = make_assertion_urn(uid)

    op_map = {
        "not_null":          AssertionStdOperatorClass.NOT_NULL,
        "accepted_values":   AssertionStdOperatorClass.IN,
        "value_between_gte": AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        "value_between_lte": AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
    }
    if kind not in op_map:
        return None, None

    if kind == "not_null":
        params = AssertionStdParametersClass()
    elif kind == "accepted_values":
        params = AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=str(kw.get("values", [])),
                type=AssertionStdParameterTypeClass.SET,
            )
        )
    else:
        params = AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=str(kw.get("value", 0)),
                type=AssertionStdParameterTypeClass.NUMBER,
            )
        )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=aurn,
        aspect=AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=table_urn,
                scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                fields=[f"urn:li:schemaField:({table_urn},{col})"],
                operator=op_map[kind],
                parameters=params,
                aggregation=AssertionStdAggregationClass.IDENTITY,
            ),
        ),
    )
    return aurn, mcp


def emit_assertions(e: DatahubRestEmitter, urn: str, c: dict) -> list[str]:
    aurns: list[str] = []
    for col, defn in c.get("columns", {}).items():
        for test in _parse_tests(defn.get("tests", [])):
            kind = test["kind"]
            if kind == "value_between":
                for suffix, val in [("value_between_gte", test["min"]),
                                     ("value_between_lte", test["max"])]:
                    aurn, mcp = _build_assertion(urn, col, suffix, value=val)
                    if aurn and mcp:
                        e.emit(mcp)
                        aurns.append(aurn)
            else:
                kw = {k: v for k, v in test.items() if k != "kind"}
                aurn, mcp = _build_assertion(urn, col, kind, **kw)
                if aurn and mcp:
                    e.emit(mcp)
                    aurns.append(aurn)
    return aurns


def emit_contract(e: DatahubRestEmitter, urn: str, aurns: list[str]) -> None:
    try:
        from datahub.metadata.schema_classes import (
            DataContractPropertiesClass,
            DataQualityContractClass,
        )
        e.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DataContractPropertiesClass(
                entity=urn,
                dataQuality=[DataQualityContractClass(assertion=a) for a in aurns],
            ),
        ))
        print(f"    ✓ DataContract ({len(aurns)} assertions)")
    except Exception as ex:
        print(f"    ⚠ DataContract skipped: {ex}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    emitter = DatahubRestEmitter(gms_server=GMS_URL)
    emitter.test_connection()
    print(f"Connected to DataHub GMS at {GMS_URL}\n")

    if not TABLE_SPECS:
        print("No contracts defined in src.pipelines.governance.suites")
        sys.exit(1)

    for table, spec in TABLE_SPECS.items():
        c     = _spec_to_contract(spec)
        urn   = _urn(table)

        print(f"\n── {PG_SCHEMA}.{table}")
        emit_properties(emitter, urn, c)
        print(f"  ✓ Description")

        emit_schema(emitter, urn, c)
        print(f"  ✓ Schema ({len(c.get('columns', {}))} columns + descriptions)")

        aurns = emit_assertions(emitter, urn, c)
        print(f"  ✓ Assertions ({len(aurns)} column tests)")

        if aurns:
            emit_contract(emitter, urn, aurns)

    print(f"\n{'='*55}")
    print("Done! → http://localhost:9002")
    print("  Table → Schema tab    : column types + descriptions")
    print("  Table → Assertions tab: per-column validation rules")
    print("  Table → Contracts tab : data contract")


if __name__ == "__main__":
    main()
