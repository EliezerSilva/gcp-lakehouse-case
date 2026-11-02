"""
Investigacao de evidencias para a quarentena.

Modo somente leitura. Nao libera nem corrige registros.
"""

from google.cloud import bigquery

PROJECT = "eli-lakehouse-gcp"
REGION = "us-central1"
MAX_BYTES = 209715200


def investigate_versions():
    client = bigquery.Client(project=PROJECT)

    if client.project != PROJECT:
        raise RuntimeError("Projeto GCP incorreto.")

    sql = f"""
    WITH q AS (
        SELECT record_hash, order_id
        FROM `{PROJECT}.quality.quarantine_records`
    ),
    b AS (
        SELECT
            order_id,
            gross_amount,
            discount_amount,
            net_amount,
            order_date,
            COUNT(*) OVER (
                PARTITION BY order_id
            ) AS versions
        FROM `{PROJECT}.bronze.erp_pedidos_cabecalho`
    ),
    valid_b AS (
        SELECT *
        FROM b
        WHERE versions > 1
          AND order_date IS NOT NULL
          AND gross_amount IS NOT NULL
          AND gross_amount >= 0
          AND discount_amount IS NOT NULL
          AND discount_amount BETWEEN 0 AND gross_amount
          AND net_amount IS NOT NULL
          AND ABS(
              net_amount - (gross_amount - discount_amount)
          ) <= 0.01
    )
    SELECT
        q.record_hash,
        COUNT(b.order_id) AS valid_versions,
        COUNTIF(
            s.order_id IS NOT NULL
            AND ABS(s.gross_amount - b.gross_amount) <= 0.01
            AND ABS(s.discount_amount - b.discount_amount) <= 0.01
            AND ABS(s.net_amount - b.net_amount) <= 0.01
            AND s.order_date = b.order_date
        ) AS silver_matches
    FROM q
    LEFT JOIN valid_b b USING (order_id)
    LEFT JOIN `{PROJECT}.silver.silver_pedidos_cabecalho` s
      ON s.order_id = b.order_id
    GROUP BY q.record_hash
    """

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES,
        use_query_cache=False,
    )

    results = []

    for row in client.query(
        sql,
        location=REGION,
        job_config=config,
    ).result():
        resolved = (
            row.valid_versions == 1
            and row.silver_matches == 1
        )

        results.append({
            "record_hash": row.record_hash,
            "state": (
                "RESOLVED_BY_APPROVED_VERSION"
                if resolved
                else "PENDING_EVIDENCE"
            ),
            "valid_versions": row.valid_versions,
            "silver_matches": row.silver_matches,
            "approved_for_correction": False,
        })

    return results


if __name__ == "__main__":
    results = investigate_versions()

    resolved = sum(
        r["state"] == "RESOLVED_BY_APPROVED_VERSION"
        for r in results
    )

    print("=== QUARANTINE EVIDENCE AGENT ===")
    print("Registros:", len(results))
    print("Representados por versao aprovada:", resolved)
    print("Pendentes:", len(results) - resolved)

    if len(results) != 19 or resolved != 2:
        raise RuntimeError(
            "Resultado inesperado. Revisar evidencias."
        )

    print("PASS: evidencias validadas.")
    print("Nenhuma correcao aplicada.")
