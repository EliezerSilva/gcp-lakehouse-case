# =============================================================================
# 03_silver_integrate.py
# Camada SILVER — Integração, deduplicação e validação referencial
#
# Responsabilidades:
#   - Deduplicação por chave de negócio
#   - Validação referencial (órfãos isolados em tabelas _orphan)
#   - Entidades conformadas prontas para o modelo dimensional
#   - Resolução de relacionamentos cross-fonte:
#       * customer_code (pedidos) → customer_id (clientes)
#       * seller_id (pedidos/vendedores) → canal e região via vendedores
#       * product_code (itens) → product_id (produtos)
#       * order_ref (entregas) → order_id (pedidos)
#       * customer_code (ocorrencias) → customer_id (clientes)
#
# Premissas do README:
#   - Uma entrega ativa por pedido (mais recente por delivered_at)
#   - Região SE duplicada → mantida a versão com state = SP
#   - status_order nulo não é cancelado — mantido como DESCONHECIDO
# =============================================================================

import logging
from pathlib import Path
from google.cloud import bigquery

from setup_00 import (
    PROJECT_ID,
    get_bq_client, ensure_ingestion_log, new_execution_id,
    run_query, StepLogger, logger,
)

EXECUTION_ID = new_execution_id()
LAYER = "SILVER"

P = PROJECT_ID


def create_or_replace(bq: bigquery.Client, table: str, select_sql: str, description: str) -> int:
    sql = f"CREATE OR REPLACE TABLE `{P}.{table}` AS\n{select_sql}"
    run_query(bq, sql, description)
    n = bq.query(f"SELECT COUNT(*) AS n FROM `{P}.{table}`").to_dataframe()["n"][0]
    return int(n)


# ---------------------------------------------------------------------------
# SILVER: silver_pedidos_cabecalho
# Deduplicação por order_id
# Campos Bronze: order_id, customer_code, seller_id, order_date, promised_date,
#                status_order, gross_amount, discount_amount, net_amount,
#                payment_details
# ---------------------------------------------------------------------------

QUALITY_GATE_PUBLISH_SQL = (
    Path(__file__).resolve().parent.parent
    / "quality_gate"
    / "03_publish_approved_orders.sql"
)

if not QUALITY_GATE_PUBLISH_SQL.is_file():
    raise FileNotFoundError(
        f"SQL de publicação do Quality Gate não encontrado: "
        f"{QUALITY_GATE_PUBLISH_SQL}"
    )

approved_sql = QUALITY_GATE_PUBLISH_SQL.read_text(
    encoding="utf-8"
).strip().rstrip(";")

# O SQL versionado é a fonte única dos critérios de aprovação.
SQL_SILVER_PEDIDOS_CAB = f"""
WITH approved AS (
{approved_sql}
)
SELECT
    order_id,
    customer_code,
    seller_id,
    order_date,
    promised_date,
    status_order,
    gross_amount,
    discount_amount,
    net_amount,
    payment_details,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM approved
"""

# ---------------------------------------------------------------------------
# SILVER: silver_pedidos_itens
# Deduplicação por (order_id, item_seq)
# Campos Bronze: order_id, item_seq, product_code, quantity, unit_price,
#                total_item, total_item_original, item_status
# ---------------------------------------------------------------------------

SQL_SILVER_PEDIDOS_ITENS = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY b.order_id, b.item_seq
            ORDER BY
                CASE
                    WHEN b.quantity > 0
                     AND b.unit_price IS NOT NULL
                     AND b.unit_price >= 0
                     AND b.total_item IS NOT NULL
                    THEN 1 ELSE 0
                END DESC,
                CASE WHEN p.product_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                b._loaded_at DESC,
                TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
                    b.product_code, b.quantity, b.unit_price, b.total_item
                )))) ASC
        ) AS _rn
    FROM `{P}.bronze.erp_pedidos_itens` b
    LEFT JOIN (
        SELECT DISTINCT product_id
        FROM `{P}.silver.silver_produtos`
    ) p ON b.product_code = p.product_id
)
, approved_orders AS (
    SELECT DISTINCT order_id
    FROM `{P}.silver.silver_pedidos_cabecalho`
)
SELECT
    order_id,
    item_seq,
    product_code,
    quantity,
    unit_price,
    total_item,
    total_item_original,
    item_status,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped
INNER JOIN approved_orders USING (order_id)
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_clientes
# Deduplicação por customer_id
# Campos Bronze: customer_id, nome_cliente, email, segmento, porte, cidade,
#                estado, status_cliente, data_cadastro
# ---------------------------------------------------------------------------

SQL_SILVER_CLIENTES = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY data_cadastro DESC NULLS LAST) AS _rn
    FROM `{P}.bronze.crm_clientes`
)
SELECT
    customer_id,
    nome_cliente,
    email,
    segmento,
    porte,
    cidade,
    estado,
    status_cliente,
    data_cadastro,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_canais
# Campos Bronze: canal_id, nome_canal, tipo_canal, ativo, observacao
# ---------------------------------------------------------------------------

SQL_SILVER_CANAIS = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY canal_id ORDER BY _loaded_at DESC) AS _rn
    FROM `{P}.bronze.comercial_canais`
)
SELECT
    canal_id,
    nome_canal,
    tipo_canal,
    ativo,
    observacao,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_vendedores
# Deduplicação por seller_id
# Campos Bronze: seller_id, seller_name, canal_id, regional_code,
#                hire_date, status
# ---------------------------------------------------------------------------

SQL_SILVER_VENDEDORES = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY _loaded_at DESC) AS _rn
    FROM `{P}.bronze.vendedores`
)
SELECT
    seller_id,
    seller_name,
    canal_id,
    regional_code,
    hire_date,
    status,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_regioes
# Premissa: SE duplicada → mantém state = SP (conforme README)
# Campos Bronze: regional_code, regional_name, state, manager_name, active_flag
# ---------------------------------------------------------------------------

SQL_SILVER_REGIOES = f"""
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY regional_code
            ORDER BY
                CASE WHEN UPPER(state) = 'SP' THEN 0 ELSE 1 END,
                _loaded_at DESC
        ) AS _rn
    FROM `{P}.bronze.regioes`
    WHERE regional_code IS NOT NULL
)
SELECT
    regional_code,
    regional_name,
    state,
    manager_name,
    active_flag,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM ranked
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_produtos
# Deduplicação por product_id (= product_product_id do RAW)
# Campos Bronze: product_id, product_name, category, subcategory,
#                product_status, list_price, currency, family, tags
# ---------------------------------------------------------------------------

SQL_SILVER_PRODUTOS = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) AS _rn
    FROM `{P}.bronze.produtos`
)
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    product_status,
    list_price,
    currency,
    family,
    tags,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_entregas
# Premissa: uma entrega ativa por pedido (mais recente por delivered_at)
# Join por order_ref → order_id: order_ref em logistica_entregas é STRING
# que corresponde ao order_id (INT64) em pedidos_cabecalho
# Campos Bronze: delivery_id, order_ref, delivery_status, cost, carrier_name,
#                carrier_mode, shipped_at, delivered_at,
#                destination_state, destination_city
# ---------------------------------------------------------------------------

SQL_SILVER_ENTREGAS = f"""
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_ref
            ORDER BY delivered_at DESC NULLS LAST, delivery_id DESC
        ) AS _rn
    FROM `{P}.bronze.logistica_entregas`
)
SELECT
    delivery_id,
    order_ref,
    -- order_ref é o identificador do pedido em logistica_entregas (STRING)
    -- mantido como STRING para join com silver_pedidos_cabecalho.order_id
    order_ref           AS order_id,
    delivery_status,
    cost              AS freight_cost,
    carrier_name,
    carrier_mode,
    shipped_at,
    delivered_at,
    destination_state,
    destination_city,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM ranked
WHERE _rn = 1
"""

# ---------------------------------------------------------------------------
# SILVER: silver_ocorrencias
# Deduplicação por ticket_id
# Validação referencial: order_id deve existir em silver_pedidos_cabecalho
# Campos Bronze: ticket_id, order_id, customer_code, event_type, severity,
#                status, metadata_channel, metadata_sla_hours, created_at
# ---------------------------------------------------------------------------

SQL_SILVER_OCORRENCIAS = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY created_at DESC NULLS LAST) AS _rn
    FROM `{P}.bronze.atendimento_ocorrencias`
),
valid_orders AS (
    SELECT DISTINCT order_id FROM `{P}.silver.silver_pedidos_cabecalho`
)
SELECT
    o.ticket_id,
    o.order_id,
    o.customer_code,
    o.event_type,
    o.severity,
    o.status,
    o.metadata_channel,
    o.metadata_sla_hours,
    o.created_at,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped o
INNER JOIN valid_orders v ON o.order_id = v.order_id
WHERE o._rn = 1
"""

SQL_SILVER_OCORRENCIAS_ORPHAN = f"""
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY created_at DESC NULLS LAST) AS _rn
    FROM `{P}.bronze.atendimento_ocorrencias`
),
valid_orders AS (
    SELECT DISTINCT order_id FROM `{P}.silver.silver_pedidos_cabecalho`
)
SELECT
    o.ticket_id,
    o.order_id,
    o.customer_code,
    o.event_type,
    o.severity,
    o.status,
    o.metadata_channel,
    o.metadata_sla_hours,
    o.created_at,
    CASE
        WHEN r.order_id IS NULL
            THEN 'PARENT_ORDER_MISSING'
        ELSE 'PARENT_ORDER_NOT_APPROVED'
    END AS orphan_reason,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped o
LEFT JOIN (
    SELECT DISTINCT order_id
    FROM `{P}.bronze.erp_pedidos_cabecalho`
) r ON o.order_id = r.order_id
LEFT JOIN valid_orders v ON o.order_id = v.order_id
WHERE o._rn = 1
  AND v.order_id IS NULL
"""

# ---------------------------------------------------------------------------
# SILVER: itens sem pedido pai (órfãos referencial)
# Colunas explícitas — sem SELECT * para evitar duplicata de _loaded_at
# ---------------------------------------------------------------------------

SQL_SILVER_ITENS_ORPHAN = f"""
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, item_seq
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM `{P}.bronze.erp_pedidos_itens`
),
raw_orders AS (
    SELECT DISTINCT order_id
    FROM `{P}.bronze.erp_pedidos_cabecalho`
)
SELECT
    i.order_id,
    i.item_seq,
    i.product_code,
    i.quantity,
    i.unit_price,
    i.total_item,
    i.total_item_original,
    i.item_status,
    CASE
        WHEN r.order_id IS NULL
            THEN 'PARENT_ORDER_MISSING'
        ELSE 'PARENT_ORDER_NOT_APPROVED'
    END AS orphan_reason,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deduped i
LEFT JOIN `{P}.silver.silver_pedidos_cabecalho` c
    ON i.order_id = c.order_id
LEFT JOIN raw_orders r
    ON i.order_id = r.order_id
WHERE i._rn = 1
  AND c.order_id IS NULL
"""

# ---------------------------------------------------------------------------
# Pipeline SILVER
# ---------------------------------------------------------------------------

def run_silver_integrate():
    bq = get_bq_client()
    ensure_ingestion_log(bq)
    logger.info(f"=== SILVER Integration | execution_id={EXECUTION_ID} ===")

    steps = [
        ("silver.silver_pedidos_cabecalho",   SQL_SILVER_PEDIDOS_CAB,        "silver_pedidos_cabecalho"),
        ("silver.silver_pedidos_itens",        SQL_SILVER_PEDIDOS_ITENS,      "silver_pedidos_itens"),
        ("silver.silver_clientes",             SQL_SILVER_CLIENTES,           "silver_clientes"),
        ("silver.silver_canais",               SQL_SILVER_CANAIS,             "silver_canais"),
        ("silver.silver_vendedores",           SQL_SILVER_VENDEDORES,         "silver_vendedores"),
        ("silver.silver_regioes",              SQL_SILVER_REGIOES,            "silver_regioes"),
        ("silver.silver_produtos",             SQL_SILVER_PRODUTOS,           "silver_produtos"),
        ("silver.silver_entregas",             SQL_SILVER_ENTREGAS,           "silver_entregas"),
        ("silver.silver_ocorrencias",          SQL_SILVER_OCORRENCIAS,        "silver_ocorrencias"),
        ("silver.silver_ocorrencias_orphan",   SQL_SILVER_OCORRENCIAS_ORPHAN, "silver_ocorrencias_orphan"),
        ("silver.silver_pedidos_itens_orphan", SQL_SILVER_ITENS_ORPHAN,       "silver_pedidos_itens_orphan"),
    ]

    for table, sql, label in steps:
        with StepLogger(bq, EXECUTION_ID, table, LAYER) as step:
            step.rows = create_or_replace(bq, table, sql, description=label)

    logger.info("=== SILVER Integration concluída ===")
    _print_summary(bq)


def _print_summary(bq: bigquery.Client):
    sql = f"""
        SELECT table_name, rows_loaded, execution_status, duration_seconds
        FROM `{P}.control.ingestion_log`
        WHERE execution_id = '{EXECUTION_ID}'
        ORDER BY execution_timestamp
    """
    df = bq.query(sql).to_dataframe()
    logger.info("\n" + df.to_string(index=False))


if __name__ == "__main__":
    run_silver_integrate()
