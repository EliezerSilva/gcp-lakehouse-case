# =============================================================================
# 00_setup.py
# Configuração central, helpers e utilitários de log
# Executar antes de qualquer notebook da pipeline
# =============================================================================
# Dependências: google-cloud-bigquery, google-cloud-storage, pandas, openpyxl
# Instalar: pip install google-cloud-bigquery google-cloud-storage pandas openpyxl db-dtypes
# =============================================================================

import os
import uuid
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery, storage

# ---------------------------------------------------------------------------
# Configuração do ambiente
# ---------------------------------------------------------------------------

PROJECT_ID   = "eli-lakehouse-gcp"
REGION       = "us-central1"
BUCKET       = "eli-lakehouse-gcp-lake"
SOURCE_PATH  = "source"

DATASETS = {
    "raw":     "raw",
    "bronze":  "bronze",
    "silver":  "silver",
    "gold":    "gold",
    "quality": "quality",
    "control": "control",
}

# Mapeamento arquivo → tabela RAW
SOURCE_MAP = {
    "erp_pedidos_cabecalho_2025.csv": "raw.erp_pedidos_cabecalho",
    "erp_pedidos_itens_2025.csv":     "raw.erp_pedidos_itens",
    "crm_clientes_export.xlsx":       "raw.crm_clientes",
    "comercial_canais.xlsx":          "raw.comercial_canais",
    "vendedores.csv":                 "raw.vendedores",
    "legado_regioes_pipe.txt":        "raw.regioes",
    "cadastro_produtos_api_dump.json":"raw.produtos",
    "logistica_entregas.json":        "raw.logistica_entregas",
    "atendimento_ocorrencias.ndjson": "raw.atendimento_ocorrencias",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("lakehouse")


# ---------------------------------------------------------------------------
# Clientes GCP
# ---------------------------------------------------------------------------

def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def get_gcs_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


# ---------------------------------------------------------------------------
# Helpers de controle de execução
# ---------------------------------------------------------------------------

def new_execution_id() -> str:
    return str(uuid.uuid4())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Registro de log no control.ingestion_log
# ---------------------------------------------------------------------------

INGESTION_LOG_TABLE = f"{PROJECT_ID}.control.ingestion_log"

INGESTION_LOG_SCHEMA = [
    bigquery.SchemaField("execution_id",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("table_name",           "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("source_file",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("rows_loaded",          "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("execution_timestamp",  "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("execution_status",     "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("error_message",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("layer",                "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("duration_seconds",     "FLOAT",     mode="NULLABLE"),
]


def ensure_ingestion_log(client: bigquery.Client) -> None:
    """Cria control.ingestion_log se não existir."""
    table_ref = bigquery.Table(INGESTION_LOG_TABLE, schema=INGESTION_LOG_SCHEMA)
    table_ref.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="execution_timestamp",
    )
    client.create_table(table_ref, exists_ok=True)
    logger.info("control.ingestion_log OK")


def log_execution(
    client: bigquery.Client,
    execution_id: str,
    table_name: str,
    layer: str,
    status: str,
    rows_loaded: Optional[int] = None,
    source_file: Optional[str] = None,
    error_message: Optional[str] = None,
    duration_seconds: Optional[float] = None,
) -> None:
    row = {
        "execution_id":       execution_id,
        "table_name":         table_name,
        "source_file":        source_file,
        "rows_loaded":        rows_loaded,
        "execution_timestamp": now_utc().isoformat(),
        "execution_status":   status,
        "error_message":      error_message,
        "layer":              layer,
        "duration_seconds":   duration_seconds,
    }
    errors = client.insert_rows_json(INGESTION_LOG_TABLE, [row])
    if errors:
        logger.warning(f"Falha ao gravar log: {errors}")


# ---------------------------------------------------------------------------
# Helper de execução de DDL/DML no BigQuery
# ---------------------------------------------------------------------------

def run_query(client: bigquery.Client, sql: str, description: str = "") -> bigquery.QueryJob:
    logger.info(f"Executando: {description or sql[:80]}")
    job = client.query(sql)
    job.result()  # bloqueia até concluir
    return job


def run_query_to_df(client: bigquery.Client, sql: str):
    import pandas as pd
    return client.query(sql).to_dataframe()


# ---------------------------------------------------------------------------
# Helper para carregar DataFrame → BigQuery
# ---------------------------------------------------------------------------

def load_df_to_bq(
    client: bigquery.Client,
    df,
    destination: str,
    write_disposition: str = "WRITE_TRUNCATE",
    schema=None,
) -> int:
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
        autodetect=(schema is None),
    )
    job = client.load_table_from_dataframe(df, destination, job_config=job_config)
    job.result()
    table = client.get_table(destination)
    return table.num_rows


# ---------------------------------------------------------------------------
# Validação de ambiente
# ---------------------------------------------------------------------------

def validate_environment(client: bigquery.Client) -> None:
    logger.info("=== Validando ambiente ===")
    for ds_name in DATASETS.values():
        ref = f"{PROJECT_ID}.{ds_name}"
        try:
            client.get_dataset(ref)
            logger.info(f"  Dataset {ref}: OK")
        except Exception as e:
            logger.error(f"  Dataset {ref}: AUSENTE — {e}")
            raise RuntimeError(f"Dataset obrigatório ausente: {ref}") from e

    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(BUCKET)
    if not bucket.exists():
        raise RuntimeError(f"Bucket ausente: gs://{BUCKET}")
    logger.info(f"  Bucket gs://{BUCKET}: OK")
    logger.info("=== Ambiente validado ===")


# ---------------------------------------------------------------------------
# Context manager para log automático de execução de etapa
# ---------------------------------------------------------------------------

class StepLogger:
    """
    Uso:
        with StepLogger(client, exec_id, "raw.erp_pedidos_cabecalho", "RAW",
                        source_file="erp_pedidos_cabecalho_2025.csv") as step:
            rows = ingest(...)
            step.rows = rows
    """
    def __init__(self, client, execution_id, table_name, layer, source_file=None):
        self.client = client
        self.execution_id = execution_id
        self.table_name = table_name
        self.layer = layer
        self.source_file = source_file
        self.rows = None
        self._start = None

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = round(time.time() - self._start, 2)
        if exc_type is None:
            logger.info(f"[OK] {self.table_name} — {self.rows} linhas — {duration}s")
            log_execution(
                self.client, self.execution_id, self.table_name, self.layer,
                status="SUCCESS", rows_loaded=self.rows,
                source_file=self.source_file, duration_seconds=duration,
            )
        else:
            logger.error(f"[ERRO] {self.table_name} — {exc_val}")
            log_execution(
                self.client, self.execution_id, self.table_name, self.layer,
                status="ERROR", rows_loaded=0,
                source_file=self.source_file, error_message=str(exc_val),
                duration_seconds=duration,
            )
        return False  # não suprime exceção


if __name__ == "__main__":
    bq = get_bq_client()
    validate_environment(bq)
    ensure_ingestion_log(bq)
    logger.info("Setup concluído.")
