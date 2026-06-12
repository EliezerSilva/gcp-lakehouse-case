# =============================================================================
# 05_quality_checks.py
# Validações cross-layer — resultados persistidos em quality.validation_results
#
# Checks implementados:
#   1. Contagem de linhas por camada (RAW → BRONZE → SILVER → GOLD)
#   2. Duplicatas por chave de negócio
#   3. Campos críticos nulos
#   4. Integridade referencial fato × dimensões
#   5. Detecção de órfãos isolados
#   6. Consistência do rateio de desconto
# =============================================================================

import logging
from datetime import datetime, timezone
from google.cloud import bigquery

from setup_00 import (
    PROJECT_ID,
    get_bq_client, ensure_ingestion_log, new_execution_id,
    run_query, logger,
)

EXECUTION_ID = new_execution_id()
P = PROJECT_ID

VALIDATION_TABLE = f"{P}.quality.validation_results"

SCHEMA_VALIDATION = [
    bigquery.SchemaField("execution_id",  "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("check_name",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("layer",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("table_name",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("metric_name",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("metric_value",  "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("threshold",     "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("status",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("details",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("executed_at",   "TIMESTAMP", mode="REQUIRED"),
]


def ensure_validation_table(bq: bigquery.Client):
    t = bigquery.Table(VALIDATION_TABLE, schema=SCHEMA_VALIDATION)
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="executed_at",
    )
    bq.create_table(t, exists_ok=True)


def persist_result(bq, check_name, layer, table_name, metric_name,
                   metric_value, status, threshold=None, details=None):
    row = {
        "execution_id":  EXECUTION_ID,
        "check_name":    check_name,
        "layer":         layer,
        "table_name":    table_name,
        "metric_name":   metric_name,
        "metric_value":  metric_value,
        "threshold":     threshold,
        "status":        status,
        "details":       details,
        "executed_at":   datetime.now(timezone.utc).isoformat(),
    }
    errors = bq.insert_rows_json(VALIDATION_TABLE, [row])
    if errors:
        logger.warning(f"Falha ao persistir resultado: {errors}")


def scalar(bq: bigquery.Client, sql: str) -> float:
    df = bq.query(sql).to_dataframe()
    return float(df.iloc[0, 0])


# ---------------------------------------------------------------------------
# Check 1 — Contagem de linhas por camada
# Thresholds baseados nos volumes confirmados pela execução RAW em produção
# ---------------------------------------------------------------------------

ROW_COUNT_CHECKS = [
    ("RAW",    "raw.erp_pedidos_cabecalho",      403),
    ("RAW",    "raw.erp_pedidos_itens",           995),
    ("RAW",    "raw.crm_clientes",                183),
    ("RAW",    "raw.comercial_canais",              8),
    ("RAW",    "raw.vendedores",                   42),
    ("RAW",    "raw.regioes",                       8),
    ("RAW",    "raw.produtos",                     72),
    ("RAW",    "raw.logistica_entregas",           322),
    ("RAW",    "raw.atendimento_ocorrencias",      270),
    ("BRONZE", "bronze.erp_pedidos_cabecalho",    403),
    ("BRONZE", "bronze.erp_pedidos_itens",        995),
    ("BRONZE", "bronze.regioes",                    7),  # XX removido
    ("BRONZE", "bronze.crm_clientes",             183),
    ("BRONZE", "bronze.vendedores",                42),
    ("BRONZE", "bronze.produtos",                  72),
    ("BRONZE", "bronze.logistica_entregas",       322),
    ("BRONZE", "bronze.atendimento_ocorrencias",  270),
    ("SILVER", "silver.silver_pedidos_cabecalho", None),
    ("SILVER", "silver.silver_pedidos_itens",     None),
    ("SILVER", "silver.silver_clientes",          None),
    ("SILVER", "silver.silver_regioes",           None),
    ("GOLD",   "gold.fato_pedidos",               None),
    ("GOLD",   "gold.dim_cliente",                None),
    ("GOLD",   "gold.dim_produto",                None),
]


def run_row_count_checks(bq: bigquery.Client):
    logger.info("=== Check 1: Contagem de linhas ===")
    for layer, table, expected in ROW_COUNT_CHECKS:
        actual = scalar(bq, f"SELECT COUNT(*) FROM `{P}.{table}`")
        if expected is None:
            status  = "PASS"
            details = f"count={int(actual)} (sem threshold)"
        elif actual >= expected:
            status  = "PASS"
            details = f"count={int(actual)}, esperado>={expected}"
        else:
            status  = "FAIL"
            details = f"count={int(actual)}, esperado>={expected}"
        logger.info(f"  [{status}] {table}: {details}")
        persist_result(bq, "row_count", layer, table,
                       "row_count", actual, status,
                       threshold=expected, details=details)


# ---------------------------------------------------------------------------
# Check 2 — Duplicatas por chave de negócio (colunas reais pós-correção)
# ---------------------------------------------------------------------------

DUPLICATE_CHECKS = [
    ("SILVER", "silver.silver_pedidos_cabecalho", "order_id"),
    ("SILVER", "silver.silver_pedidos_itens",     "order_id, item_seq"),
    ("SILVER", "silver.silver_clientes",          "customer_id"),
    ("SILVER", "silver.silver_produtos",          "product_id"),
    ("SILVER", "silver.silver_vendedores",        "seller_id"),
    ("SILVER", "silver.silver_regioes",           "regional_code"),
    ("SILVER", "silver.silver_canais",            "canal_id"),
    ("SILVER", "silver.silver_entregas",          "order_id"),
    ("GOLD",   "gold.fato_pedidos",               "sk_fato"),
    ("GOLD",   "gold.dim_cliente",                "sk_cliente"),
    ("GOLD",   "gold.dim_produto",                "sk_produto"),
    ("GOLD",   "gold.dim_canal",                  "sk_canal"),
    ("GOLD",   "gold.dim_regiao",                 "sk_regiao"),
    ("GOLD",   "gold.dim_vendedor",               "sk_vendedor"),
]


def run_duplicate_checks(bq: bigquery.Client):
    logger.info("=== Check 2: Duplicatas ===")
    for layer, table, key_cols in DUPLICATE_CHECKS:
        sql = f"""
            SELECT COUNTIF(cnt > 1) AS dup_groups
            FROM (
                SELECT {key_cols}, COUNT(*) AS cnt
                FROM `{P}.{table}`
                GROUP BY {key_cols}
            )
        """
        dup_count = scalar(bq, sql)
        status  = "PASS" if dup_count == 0 else "FAIL"
        details = f"grupos duplicados={int(dup_count)}"
        logger.info(f"  [{status}] {table} ({key_cols}): {details}")
        persist_result(bq, "duplicate_check", layer, table,
                       "duplicate_groups", dup_count, status,
                       threshold=0, details=details)


# ---------------------------------------------------------------------------
# Check 3 — Nulos em campos críticos (colunas reais pós-correção)
# ---------------------------------------------------------------------------

NULL_CHECKS = [
    ("SILVER", "silver.silver_pedidos_cabecalho", "order_id",       0.0),
    ("SILVER", "silver.silver_pedidos_cabecalho", "customer_code",  0.0),
    ("SILVER", "silver.silver_pedidos_cabecalho", "order_date",     0.05),
    ("SILVER", "silver.silver_pedidos_itens",     "order_id",       0.0),
    ("SILVER", "silver.silver_pedidos_itens",     "product_code",   0.0),
    ("SILVER", "silver.silver_pedidos_itens",     "quantity",       0.0),
    ("SILVER", "silver.silver_entregas",          "order_ref",      0.0),
    ("SILVER", "silver.silver_entregas",          "delivery_status",0.0),
    ("GOLD",   "gold.fato_pedidos",               "sk_fato",        0.0),
    ("GOLD",   "gold.fato_pedidos",               "sk_cliente",     0.1),  # customer_code → customer_id pode não casar
    ("GOLD",   "gold.fato_pedidos",               "receita_liquida_item", 0.0),
    ("GOLD",   "gold.fato_pedidos",               "sk_regiao",      0.1),  # regional_code pode não existir para todos
]


def run_null_checks(bq: bigquery.Client):
    logger.info("=== Check 3: Nulos críticos ===")
    for layer, table, col, max_pct in NULL_CHECKS:
        sql = f"""
            SELECT SAFE_DIVIDE(COUNTIF({col} IS NULL), COUNT(*)) AS null_pct
            FROM `{P}.{table}`
        """
        null_pct = scalar(bq, sql)
        status  = "PASS" if null_pct <= max_pct else "FAIL"
        details = f"null_pct={null_pct:.4f}, threshold={max_pct}"
        logger.info(f"  [{status}] {table}.{col}: {details}")
        persist_result(bq, "null_check", layer, table,
                       f"null_pct_{col}", null_pct, status,
                       threshold=max_pct, details=details)


# ---------------------------------------------------------------------------
# Check 4 — Integridade referencial fato × dimensões
# ---------------------------------------------------------------------------

REF_CHECKS = [
    ("gold.fato_pedidos", "sk_cliente",  "gold.dim_cliente",  "sk_cliente"),
    ("gold.fato_pedidos", "sk_produto",  "gold.dim_produto",  "sk_produto"),
    ("gold.fato_pedidos", "sk_canal",    "gold.dim_canal",    "sk_canal"),
    ("gold.fato_pedidos", "sk_vendedor", "gold.dim_vendedor", "sk_vendedor"),
    ("gold.fato_pedidos", "sk_regiao",   "gold.dim_regiao",   "sk_regiao"),
    ("gold.fato_pedidos", "sk_data",     "gold.dim_data",     "sk_data"),
]


def run_referential_checks(bq: bigquery.Client):
    logger.info("=== Check 4: Integridade referencial ===")
    for fato_table, fato_col, dim_table, dim_col in REF_CHECKS:
        sql = f"""
            SELECT COUNT(*) AS orphans
            FROM `{P}.{fato_table}` f
            LEFT JOIN `{P}.{dim_table}` d ON f.{fato_col} = d.{dim_col}
            WHERE f.{fato_col} IS NOT NULL AND d.{dim_col} IS NULL
        """
        orphans = scalar(bq, sql)
        status  = "PASS" if orphans == 0 else "WARN"
        details = f"fato sem dim correspondente: {int(orphans)}"
        logger.info(f"  [{status}] {fato_table}.{fato_col} → {dim_table}: {details}")
        persist_result(bq, "referential_integrity", "GOLD", fato_table,
                       f"orphans_{fato_col}", orphans, status,
                       threshold=0, details=details)


# ---------------------------------------------------------------------------
# Check 5 — Órfãos isolados na Silver
# ---------------------------------------------------------------------------

def run_orphan_checks(bq: bigquery.Client):
    logger.info("=== Check 5: Órfãos Silver ===")
    orphan_tables = [
        ("silver.silver_ocorrencias_orphan",   "ocorrencias sem pedido"),
        ("silver.silver_pedidos_itens_orphan",  "itens sem cabeçalho"),
    ]
    for table, desc in orphan_tables:
        try:
            count  = scalar(bq, f"SELECT COUNT(*) FROM `{P}.{table}`")
            status = "PASS" if count == 0 else "WARN"
            details = f"{desc}: {int(count)} registros isolados"
            logger.info(f"  [{status}] {table}: {details}")
            persist_result(bq, "orphan_check", "SILVER", table,
                           "orphan_count", count, status,
                           threshold=0, details=details)
        except Exception as e:
            logger.warning(f"  Tabela {table} não encontrada: {e}")


# ---------------------------------------------------------------------------
# Check 6 — Consistência do rateio de desconto
# ---------------------------------------------------------------------------

def run_discount_consistency_check(bq: bigquery.Client):
    logger.info("=== Check 6: Consistência do rateio de desconto ===")
    sql = f"""
        SELECT COUNTIF(ABS(soma_rateado - desconto_pedido) > 0.01) AS inconsistentes
        FROM (
            SELECT
                f.order_id,
                ROUND(SUM(f.desconto_rateado_item), 4) AS soma_rateado,
                MAX(f.desconto_pedido)                 AS desconto_pedido
            FROM `{P}.gold.fato_pedidos` f
            WHERE f.desconto_pedido IS NOT NULL
            GROUP BY f.order_id
        )
    """
    inconsistentes = scalar(bq, sql)
    status  = "PASS" if inconsistentes == 0 else "FAIL"
    details = f"pedidos com rateio inconsistente: {int(inconsistentes)}"
    logger.info(f"  [{status}] rateio_desconto: {details}")
    persist_result(bq, "discount_consistency", "GOLD", "gold.fato_pedidos",
                   "discount_rateio_inconsistentes", inconsistentes, status,
                   threshold=0, details=details)


# ---------------------------------------------------------------------------
# Check 7 — Resolução customer_code → customer_id
# Pedidos cujo customer_code não casa com nenhum cliente
# ---------------------------------------------------------------------------

def run_customer_resolution_check(bq: bigquery.Client):
    logger.info("=== Check 7: Resolução customer_code → customer_id ===")
    sql = f"""
        SELECT
            SAFE_DIVIDE(
                COUNTIF(sk_cliente IS NULL),
                COUNT(*)
            ) AS pct_sem_cliente
        FROM `{P}.gold.fato_pedidos`
    """
    pct = scalar(bq, sql)
    status  = "PASS" if pct <= 0.05 else "WARN"
    details = f"pct_pedidos_sem_cliente={pct:.4f}"
    logger.info(f"  [{status}] customer_code resolution: {details}")
    persist_result(bq, "customer_resolution", "GOLD", "gold.fato_pedidos",
                   "pct_sem_cliente", pct, status,
                   threshold=0.05, details=details)


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def print_summary(bq: bigquery.Client):
    sql = f"""
        SELECT
            check_name,
            layer,
            COUNTIF(status = 'PASS') AS pass_count,
            COUNTIF(status = 'FAIL') AS fail_count,
            COUNTIF(status = 'WARN') AS warn_count
        FROM `{VALIDATION_TABLE}`
        WHERE execution_id = '{EXECUTION_ID}'
        GROUP BY 1, 2
        ORDER BY 2, 1
    """
    df = bq.query(sql).to_dataframe()
    logger.info("\n=== QUALITY SUMMARY ===\n" + df.to_string(index=False))
    total_fail = df["fail_count"].sum()
    if total_fail > 0:
        logger.error(f"ATENÇÃO: {int(total_fail)} check(s) com status FAIL")
    else:
        logger.info("Todos os checks PASS/WARN — pipeline validada.")


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

def run_quality_checks():
    bq = get_bq_client()
    ensure_ingestion_log(bq)
    ensure_validation_table(bq)
    logger.info(f"=== Quality Checks | execution_id={EXECUTION_ID} ===")

    run_row_count_checks(bq)
    run_duplicate_checks(bq)
    run_null_checks(bq)
    run_referential_checks(bq)
    run_orphan_checks(bq)
    run_discount_consistency_check(bq)
    run_customer_resolution_check(bq)

    print_summary(bq)
    logger.info("=== Quality Checks concluídos ===")


if __name__ == "__main__":
    run_quality_checks()
