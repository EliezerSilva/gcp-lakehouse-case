"""
Ferramenta de investigacao de evidencias Bronze.

Somente leitura.
Nenhuma correcao ou liberacao automatica.
"""

from google.cloud import bigquery
from importlib import import_module

identity = import_module("11_evidence_identity")

PROJECT = "eli-lakehouse-gcp"
REGION = "us-central1"
MAX_BYTES = 209715200


def fetch_evidence(record_hash, client=None):
    if not isinstance(record_hash, str) or len(record_hash) != 64:
        raise ValueError("record_hash invalido.")

    client = client or bigquery.Client(project=PROJECT)

    if client.project != PROJECT:
        raise RuntimeError("Projeto GCP incorreto.")

    sql = f"""
    WITH target AS (
        SELECT DISTINCT order_id
        FROM `{PROJECT}.quality.quarantine_records`
        WHERE record_hash = @record_hash
    )
    SELECT
        b.order_id,
        b.order_date,
        b.gross_amount,
        b.discount_amount,
        b.net_amount,
        b.source_updated_at
    FROM `{PROJECT}.bronze.erp_pedidos_cabecalho` b
    INNER JOIN target t
        ON b.order_id = t.order_id
    ORDER BY b.source_updated_at DESC
    LIMIT 20
    """

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "record_hash", "STRING", record_hash
            )
        ],
        maximum_bytes_billed=MAX_BYTES,
        use_query_cache=False,
    )

    rows = client.query(
        sql,
        location=REGION,
        job_config=config,
    ).result()

    results = []

    for row in rows:
        values = {
            "order_id": row.order_id,
            "order_date": str(row.order_date),
            "gross_amount": row.gross_amount,
            "discount_amount": row.discount_amount,
            "net_amount": row.net_amount,
        }

        evidence = identity.canonical_evidence(
            source=f"{PROJECT}.bronze.erp_pedidos_cabecalho",
            record_hash=record_hash,
            version=str(row.source_updated_at),
            values=values,
        )

        evidence["validation_status"] = "UNVERIFIED"
        evidence["approved_for_correction"] = False

        results.append(evidence)

    return results


def validate_bronze_values(values):
    """
    Valida campos financeiros e data.

    Reproduz as condicoes DQ002-DQ005.
    Nao certifica a procedencia dos dados.
    """

    from decimal import Decimal, InvalidOperation

    def number(value):
        if value is None:
            return None
        try:
            result = Decimal(str(value))
            return result if result.is_finite() else None
        except (InvalidOperation, ValueError, TypeError):
            return None

    gross = number(values.get("gross_amount"))
    discount = number(values.get("discount_amount"))
    net = number(values.get("net_amount"))
    date = values.get("order_date")

    errors = []

    if date in (None, "", "None"):
        errors.append("DQ002_ORDER_DATE_INVALID")

    if gross is None or gross < 0:
        errors.append("DQ003_GROSS_AMOUNT_INVALID")

    if (
        discount is None
        or gross is None
        or discount < 0
        or discount > gross
    ):
        errors.append("DQ004_DISCOUNT_INVALID")

    if (
        net is None
        or gross is None
        or discount is None
        or abs(net - (gross - discount)) > Decimal("0.01")
    ):
        errors.append("DQ005_NET_AMOUNT_INVALID")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "approved_for_correction": False,
    }
