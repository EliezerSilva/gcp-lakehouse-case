"""
Quality Gate bloqueante entre Bronze e Silver.

Materializa a quarentena e valida sua consistencia.
Uma falha interrompe a execucao.
"""

from pathlib import Path

from google.cloud import bigquery
from setup_00 import PROJECT_ID, REGION, get_bq_client, logger

QUALITY_GATE_DIR = Path(__file__).resolve().parent.parent / "quality_gate"


def run_quality_gate():
    client = get_bq_client()

    steps = (
        "02_materialize_quarantine.sql",
        "04_assert_quarantine.sql",
    )

    expected_project = "eli-lakehouse-gcp"

    if PROJECT_ID != expected_project:
        raise RuntimeError(
            f"Projeto inesperado: {PROJECT_ID}. "
            f"Esperado: {expected_project}"
        )

    if client.project != expected_project:
        raise RuntimeError(
            f"Cliente BigQuery conectado ao projeto incorreto: "
            f"{client.project}"
        )

    missing = [
        name for name in steps
        if not (QUALITY_GATE_DIR / name).is_file()
    ]

    if missing:
        raise FileNotFoundError(
            f"Arquivos obrigatorios ausentes: {missing}"
        )

    for filename in steps:
        path = QUALITY_GATE_DIR / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Arquivo obrigatorio do Quality Gate ausente: {path}"
            )

        sql = path.read_text(encoding="utf-8").strip()

        if not sql:
            raise RuntimeError(f"SQL vazio: {path}")

        expected_project = "eli-lakehouse-gcp"

        if PROJECT_ID != expected_project:
            raise RuntimeError(
                f"Projeto GCP inesperado: {PROJECT_ID}. "
                f"Esperado: {expected_project}"
            )

        config = bigquery.QueryJobConfig(
            maximum_bytes_billed=100_000_000
        )

        logger.info("Quality Gate: executando %s", filename)

        job = client.query(
            sql,
            location=REGION,
            job_config=config,
        )
        job.result()

        logger.info("Quality Gate: %s concluido", filename)

    logger.info("Quality Gate aprovado. Silver liberada.")
