# =============================================================================
# 01_raw_ingestion.py
# Camada RAW — Ingestão fiel dos 9 arquivos fonte do GCS para BigQuery
#
# Regras:
#   - Nenhuma transformação de negócio
#   - Campos inconsistentes como STRING
#   - Preserva nulos e inconsistências
#   - Idempotente (WRITE_TRUNCATE)
#   - Registra execução em control.ingestion_log
# =============================================================================

import io
import json
import time
import logging

import pandas as pd
from google.cloud import bigquery, storage

# Importa setup central (assumindo execução no mesmo diretório ou PYTHONPATH configurado)
from setup_00 import (
    PROJECT_ID, BUCKET, SOURCE_PATH,
    get_bq_client, get_gcs_client,
    ensure_ingestion_log, new_execution_id,
    load_df_to_bq, StepLogger, logger,
)

EXECUTION_ID = new_execution_id()
LAYER = "RAW"


# ---------------------------------------------------------------------------
# Helpers de leitura do GCS
# ---------------------------------------------------------------------------

def read_gcs_bytes(gcs_client: storage.Client, filename: str) -> bytes:
    bucket = gcs_client.bucket(BUCKET)
    blob = bucket.blob(f"{SOURCE_PATH}/{filename}")
    return blob.download_as_bytes()


def all_string_schema(df: pd.DataFrame):
    """Converte todas as colunas para STRING antes de carregar no RAW."""
    return df.astype(str).where(df.notna(), other=None)


# ---------------------------------------------------------------------------
# Ingestores por tipo de fonte
# ---------------------------------------------------------------------------

def ingest_csv(
    bq: bigquery.Client,
    gcs: storage.Client,
    filename: str,
    destination: str,
    sep: str = ",",
    encoding: str = "utf-8",
) -> int:
    raw_bytes = read_gcs_bytes(gcs, filename)
    df = pd.read_csv(
        io.BytesIO(raw_bytes),
        sep=sep,
        dtype=str,          # tudo STRING — regra RAW
        keep_default_na=False,
        encoding=encoding,
    )
    df = df.where(df != "", other=None)  # strings vazias → NULL
    return load_df_to_bq(bq, df, destination, write_disposition="WRITE_TRUNCATE")


def ingest_xlsx(
    bq: bigquery.Client,
    gcs: storage.Client,
    filename: str,
    destination: str,
) -> int:
    raw_bytes = read_gcs_bytes(gcs, filename)
    df = pd.read_excel(
        io.BytesIO(raw_bytes),
        dtype=str,
        keep_default_na=False,
    )
    df = df.where(df != "", other=None)
    return load_df_to_bq(bq, df, destination, write_disposition="WRITE_TRUNCATE")


def ingest_txt_pipe(
    bq: bigquery.Client,
    gcs: storage.Client,
    filename: str,
    destination: str,
) -> int:
    raw_bytes = read_gcs_bytes(gcs, filename)
    df = pd.read_csv(
        io.BytesIO(raw_bytes),
        sep=r"\|",
        dtype=str,
        keep_default_na=False,
        engine="python",
    )
    df = df.where(df != "", other=None)
    return load_df_to_bq(bq, df, destination, write_disposition="WRITE_TRUNCATE")


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitiza nomes de colunas para compatibilidade com BigQuery.
    json_normalize produz nomes com '.' para campos aninhados (ex: product.product_id).
    BigQuery rejeita qualquer caractere que não seja [a-zA-Z0-9_].
    Aplicado exclusivamente em ingestores JSON/NDJSON — CSV/XLSX/TXT não são afetados.
    """
    import re
    def _clean(col: str) -> str:
        col = col.replace(".", "_")   # nested: product.id → product_id
        col = col.replace(" ", "_")   # espaços
        col = col.replace("-", "_")   # hífens
        col = re.sub(r"[^\w]", "_", col)   # qualquer outro caractere inválido
        col = re.sub(r"_+", "_", col)      # colapsa underscores múltiplos
        col = col.strip("_")               # remove underscores nas bordas
        return col
    df.columns = [_clean(c) for c in df.columns]
    return df


def ingest_json_array(
    bq: bigquery.Client,
    gcs: storage.Client,
    filename: str,
    destination: str,
) -> int:
    raw_bytes = read_gcs_bytes(gcs, filename)
    data = json.loads(raw_bytes.decode("utf-8"))
    df = pd.json_normalize(data)
    df = sanitize_columns(df)
    # Normaliza para STRING conforme regra RAW
    df = df.astype(str).where(df.notna(), other=None)
    df = df.where(df != "None", other=None)
    return load_df_to_bq(bq, df, destination, write_disposition="WRITE_TRUNCATE")


def ingest_ndjson(
    bq: bigquery.Client,
    gcs: storage.Client,
    filename: str,
    destination: str,
) -> int:
    raw_bytes = read_gcs_bytes(gcs, filename)
    lines = raw_bytes.decode("utf-8").strip().splitlines()
    records = [json.loads(line) for line in lines if line.strip()]
    df = pd.json_normalize(records)
    df = sanitize_columns(df)
    df = df.astype(str).where(df.notna(), other=None)
    df = df.where(df != "None", other=None)
    return load_df_to_bq(bq, df, destination, write_disposition="WRITE_TRUNCATE")


# ---------------------------------------------------------------------------
# Pipeline RAW principal
# ---------------------------------------------------------------------------

def run_raw_ingestion():
    bq  = get_bq_client()
    gcs = get_gcs_client()

    ensure_ingestion_log(bq)
    logger.info(f"=== RAW Ingestion | execution_id={EXECUTION_ID} ===")

    # -- 1. erp_pedidos_cabecalho (CSV, sep=;) --------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.erp_pedidos_cabecalho", LAYER,
                    source_file="erp_pedidos_cabecalho_2025.csv") as step:
        step.rows = ingest_csv(
            bq, gcs,
            filename="erp_pedidos_cabecalho_2025.csv",
            destination=f"{PROJECT_ID}.raw.erp_pedidos_cabecalho",
            sep=";",
        )

    # -- 2. erp_pedidos_itens (CSV, sep=,) ------------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.erp_pedidos_itens", LAYER,
                    source_file="erp_pedidos_itens_2025.csv") as step:
        step.rows = ingest_csv(
            bq, gcs,
            filename="erp_pedidos_itens_2025.csv",
            destination=f"{PROJECT_ID}.raw.erp_pedidos_itens",
            sep=",",
        )

    # -- 3. crm_clientes (XLSX) ------------------------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.crm_clientes", LAYER,
                    source_file="crm_clientes_export.xlsx") as step:
        step.rows = ingest_xlsx(
            bq, gcs,
            filename="crm_clientes_export.xlsx",
            destination=f"{PROJECT_ID}.raw.crm_clientes",
        )

    # -- 4. comercial_canais (XLSX) --------------------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.comercial_canais", LAYER,
                    source_file="comercial_canais.xlsx") as step:
        step.rows = ingest_xlsx(
            bq, gcs,
            filename="comercial_canais.xlsx",
            destination=f"{PROJECT_ID}.raw.comercial_canais",
        )

    # -- 5. vendedores (CSV, sep=;) -------------------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.vendedores", LAYER,
                    source_file="vendedores.csv") as step:
        step.rows = ingest_csv(
            bq, gcs,
            filename="vendedores.csv",
            destination=f"{PROJECT_ID}.raw.vendedores",
            sep=";",
        )

    # -- 6. regioes (TXT, sep=|) ----------------------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.regioes", LAYER,
                    source_file="legado_regioes_pipe.txt") as step:
        step.rows = ingest_txt_pipe(
            bq, gcs,
            filename="legado_regioes_pipe.txt",
            destination=f"{PROJECT_ID}.raw.regioes",
        )

    # -- 7. produtos (JSON array) ----------------------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.produtos", LAYER,
                    source_file="cadastro_produtos_api_dump.json") as step:
        step.rows = ingest_json_array(
            bq, gcs,
            filename="cadastro_produtos_api_dump.json",
            destination=f"{PROJECT_ID}.raw.produtos",
        )

    # -- 8. logistica_entregas (JSON array) -----------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.logistica_entregas", LAYER,
                    source_file="logistica_entregas.json") as step:
        step.rows = ingest_json_array(
            bq, gcs,
            filename="logistica_entregas.json",
            destination=f"{PROJECT_ID}.raw.logistica_entregas",
        )

    # -- 9. atendimento_ocorrencias (NDJSON) ----------------------------------
    with StepLogger(bq, EXECUTION_ID, "raw.atendimento_ocorrencias", LAYER,
                    source_file="atendimento_ocorrencias.ndjson") as step:
        step.rows = ingest_ndjson(
            bq, gcs,
            filename="atendimento_ocorrencias.ndjson",
            destination=f"{PROJECT_ID}.raw.atendimento_ocorrencias",
        )

    logger.info("=== RAW Ingestion concluída ===")
    _print_summary(bq)


def _print_summary(bq: bigquery.Client):
    sql = f"""
        SELECT table_name, rows_loaded, execution_status, duration_seconds
        FROM `{PROJECT_ID}.control.ingestion_log`
        WHERE execution_id = '{EXECUTION_ID}'
        ORDER BY execution_timestamp
    """
    df = bq.query(sql).to_dataframe()
    logger.info("\n" + df.to_string(index=False))


if __name__ == "__main__":
    run_raw_ingestion()
