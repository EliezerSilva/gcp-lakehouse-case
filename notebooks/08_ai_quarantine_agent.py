"""
AI Quarantine Recovery Agent - fase deterministica.

Modo exclusivo: diagnostico.
Sem alteracoes em BigQuery.
Sem chamadas a modelos generativos.
"""

import json
from decimal import Decimal, InvalidOperation
from google.cloud import bigquery

PROJECT = "eli-lakehouse-gcp"
REGION = "us-central1"
MAX_BYTES = 209715200


def decimal_value(value):
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def diagnose(row):
    errors = list(row["quality_errors"])
    payload = json.loads(row["record_payload"])

    gross = decimal_value(payload.get("gross_amount"))
    discount = decimal_value(payload.get("discount_amount"))
    net = decimal_value(payload.get("net_amount"))

    recommendations = []

    if "DQ005_NET_AMOUNT_INVALID" in errors:
        expected = (
            gross - discount
            if gross is not None and discount is not None
            else None
        )
        recommendations.append({
            "rule": "DQ005",
            "action": "VERIFY_NET_AMOUNT_WITH_SOURCE",
            "calculated_net": (
                str(expected) if expected is not None else None
            ),
            "automatic_correction": False,
        })

    if "DQ004_DISCOUNT_INVALID" in errors:
        recommendations.append({
            "rule": "DQ004",
            "action": "INVESTIGATE_ORIGINAL_DISCOUNT",
            "automatic_correction": False,
        })

    if "DQ003_GROSS_AMOUNT_INVALID" in errors:
        recommendations.append({
            "rule": "DQ003",
            "action": "INVESTIGATE_ORIGINAL_GROSS_AMOUNT",
            "automatic_correction": False,
        })

    if "DQ002_ORDER_DATE_INVALID" in errors:
        recommendations.append({
            "rule": "DQ002",
            "action": "RECOVER_DATE_FROM_TRUSTED_SOURCE",
            "automatic_correction": False,
        })

    return {
        "record_hash": row["record_hash"],
        "state": "PENDING_EVIDENCE",
        "attempt": 1,
        "errors": errors,
        "recommendations": recommendations,
        "approved": False,
    }


def main():
    client = bigquery.Client(project=PROJECT)

    if client.project != PROJECT:
        raise RuntimeError("Projeto GCP incorreto.")

    query = f"""
    SELECT record_hash, quality_errors, record_payload
    FROM `{PROJECT}.quality.quarantine_records`
    ORDER BY record_hash
    """

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES,
        use_query_cache=False,
    )

    rows = client.query(
        query,
        location=REGION,
        job_config=config,
    ).result()

    print("=== AI QUARANTINE AGENT - DIAGNOSTICO ===")

    total = 0

    for row in rows:
        diagnosis = diagnose(dict(row.items()))
        print(json.dumps(diagnosis, ensure_ascii=False))
        total += 1

    print(f"Registros analisados: {total}")
    print("Nenhuma correcao aplicada.")
    print("Nenhum dado enviado a modelo de IA.")


if __name__ == "__main__":
    main()
