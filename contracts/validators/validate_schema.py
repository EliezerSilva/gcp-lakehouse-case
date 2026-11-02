"""Valida contrato YAML contra o schema físico do BigQuery."""

import argparse
from pathlib import Path

import yaml
from google.cloud import bigquery

PROJECT = "eli-lakehouse-gcp"


def validate(contract_path):
    contract = yaml.safe_load(
        Path(contract_path).read_text(encoding="utf-8")
    )

    if contract["contract_status"] != "DRAFT_NOT_ENFORCED":
        raise RuntimeError("Estado contratual inesperado.")

    dataset = contract["dataset"]

    if dataset["project"] != PROJECT:
        raise RuntimeError("Projeto GCP divergente.")

    client = bigquery.Client(project=PROJECT)

    target = f"{PROJECT}.{dataset['target']}"
    table = client.get_table(target)

    expected = {
        field["name"]: field["type"].upper()
        for field in contract["schema"]
    }

    actual = {
        field.name: field.field_type.upper()
        for field in table.schema
    }

    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))

    type_mismatches = {
        name: {
            "expected": expected[name],
            "actual": actual[name],
        }
        for name in set(expected) & set(actual)
        if expected[name] != actual[name]
    }

    print("=== DATA CONTRACT — SCHEMA VALIDATION ===")
    print("Contrato:", contract_path)
    print("Tabela:", target)
    print("Colunas no contrato:", len(expected))
    print("Colunas no BigQuery:", len(actual))
    print("Ausentes:", missing)
    print("Extras:", extra)
    print("Tipos divergentes:", type_mismatches)

    if missing or extra or type_mismatches:
        raise RuntimeError("FAIL: schema drift detectado.")

    print("PASS: schema fisico compativel com o contrato.")
    print("Contrato permanece DRAFT_NOT_ENFORCED.")
    print("Nenhuma tabela foi alterada.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", required=True)
    args = parser.parse_args()

    validate(args.contract)
