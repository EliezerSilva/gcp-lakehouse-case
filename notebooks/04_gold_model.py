# =============================================================================
# 04_gold_model.py
# Camada GOLD — Modelo dimensional + métricas pré-agregadas + view de consumo
#
# Entregáveis:
#   Dimensões: dim_cliente, dim_produto, dim_canal, dim_regiao,
#              dim_vendedor, dim_data
#   Fato:      fato_pedidos (1 linha por item de pedido)
#   Métricas:  metricas_por_periodo, metricas_por_regiao, metricas_por_canal,
#              metricas_por_categoria, metricas_operacionais
#   View:      vw_kpis_pedidos (ponto único de consumo BI)
#
# Decisões técnicas (README):
#   - Surrogate keys via SHA256 (determinístico no reprocessamento)
#   - receita_liquida_item: desconto rateado proporcionalmente por item
#   - Join cabecalho × itens ocorre aqui (não na Silver)
#   - dim_data gerada dinamicamente do range efetivo ±30 dias
#   - cancelados filtrados na view, não na fato
#
# Relacionamentos cross-fonte resolvidos aqui:
#   - customer_code (pedidos) → customer_id (silver_clientes)
#   - seller_id (pedidos) → canal_id e regional_code (silver_vendedores)
#   - regional_code (vendedores) → regional_code (silver_regioes)
#   - product_code (itens) → product_id (silver_produtos)
#   - order_id (pedidos) → order_id (silver_entregas, via SAFE_CAST(order_ref))
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
LAYER = "GOLD"

P = PROJECT_ID


def create_or_replace(bq: bigquery.Client, table: str, select_sql: str, description: str) -> int:
    sql = f"CREATE OR REPLACE TABLE `{P}.{table}` AS\n{select_sql}"
    run_query(bq, sql, description)
    n = bq.query(f"SELECT COUNT(*) AS n FROM `{P}.{table}`").to_dataframe()["n"][0]
    return int(n)


def create_or_replace_view(bq: bigquery.Client, view_id: str, select_sql: str, description: str):
    sql = f"CREATE OR REPLACE VIEW `{P}.{view_id}` AS\n{select_sql}"
    run_query(bq, sql, description)


# ---------------------------------------------------------------------------
# dim_cliente
# Source: silver_clientes
# Chave de negócio: customer_id (INT64)
# ---------------------------------------------------------------------------

SQL_DIM_CLIENTE = f"""
SELECT
    TO_HEX(SHA256(customer_id)) AS sk_cliente,
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
FROM `{P}.silver.silver_clientes`
"""

# ---------------------------------------------------------------------------
# dim_produto
# Source: silver_produtos
# Chave de negócio: product_id (STRING — vem do JSON como código alfanumérico)
# ---------------------------------------------------------------------------

SQL_DIM_PRODUTO = f"""
SELECT
    TO_HEX(SHA256(product_id))  AS sk_produto,
    product_id,
    product_name  AS nome_produto,
    category      AS categoria,
    subcategory   AS subcategoria,
    product_status AS status_produto,
    list_price    AS preco_lista,
    currency      AS moeda,
    family        AS familia,
    tags,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.silver.silver_produtos`
"""

# ---------------------------------------------------------------------------
# dim_canal
# Source: silver_canais
# Chave de negócio: canal_id (INT64)
# ---------------------------------------------------------------------------

SQL_DIM_CANAL = f"""
SELECT
    TO_HEX(SHA256(canal_id)) AS sk_canal,
    canal_id,
    nome_canal,
    tipo_canal,
    ativo,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.silver.silver_canais`
"""

# ---------------------------------------------------------------------------
# dim_regiao
# Source: silver_regioes
# Chave de negócio: regional_code (STRING)
# ---------------------------------------------------------------------------

SQL_DIM_REGIAO = f"""
SELECT
    TO_HEX(SHA256(regional_code)) AS sk_regiao,
    regional_code,
    regional_name AS nome_regiao,
    state         AS estado,
    manager_name  AS gestor,
    active_flag   AS ativo,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.silver.silver_regioes`
"""

# ---------------------------------------------------------------------------
# dim_vendedor
# Source: silver_vendedores
# Chave de negócio: seller_id (INT64)
# ---------------------------------------------------------------------------

SQL_DIM_VENDEDOR = f"""
SELECT
    TO_HEX(SHA256(seller_id)) AS sk_vendedor,
    seller_id,
    seller_name   AS nome_vendedor,
    canal_id,
    regional_code AS codigo_regional,
    status,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.silver.silver_vendedores`
"""

# ---------------------------------------------------------------------------
# dim_data
# Gerada dinamicamente do range efetivo de order_date ±30 dias
# ---------------------------------------------------------------------------

SQL_DIM_DATA = f"""
WITH date_range AS (
    SELECT
        DATE_SUB(MIN(order_date), INTERVAL 30 DAY) AS start_date,
        DATE_ADD(MAX(order_date), INTERVAL 30 DAY) AS end_date
    FROM `{P}.silver.silver_pedidos_cabecalho`
    WHERE order_date IS NOT NULL
),
date_spine AS (
    SELECT GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY) AS dates
    FROM date_range
),
exploded AS (
    SELECT d AS data_completa
    FROM date_spine, UNNEST(dates) AS d
)
SELECT
    TO_HEX(SHA256(CAST(data_completa AS STRING))) AS sk_data,
    data_completa,
    EXTRACT(YEAR      FROM data_completa) AS ano,
    EXTRACT(QUARTER   FROM data_completa) AS trimestre,
    EXTRACT(MONTH     FROM data_completa) AS mes,
    FORMAT_DATE('%B', data_completa)      AS nome_mes,
    EXTRACT(WEEK      FROM data_completa) AS semana_ano,
    EXTRACT(DAY       FROM data_completa) AS dia,
    EXTRACT(DAYOFWEEK FROM data_completa) AS dia_semana_numero,
    FORMAT_DATE('%A', data_completa)      AS dia_semana_nome,
    IF(EXTRACT(DAYOFWEEK FROM data_completa) IN (1,7), TRUE, FALSE) AS fim_de_semana,
    FORMAT_DATE('%Y-%m', data_completa)   AS ano_mes,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM exploded
"""

# ---------------------------------------------------------------------------
# fato_pedidos
# Granularidade: 1 linha por item de pedido (order_id × item_seq)
# Resolve todos os relacionamentos cross-fonte neste ponto:
#   customer_code → customer_id via silver_clientes
#   seller_id → canal_id e regional_code via silver_vendedores
#   regional_code → sk_regiao via silver_regioes
#   product_code → sk_produto via silver_produtos
# ---------------------------------------------------------------------------

SQL_FATO_PEDIDOS = f"""
WITH itens_com_peso AS (
    SELECT
        i.order_id,
        i.item_seq,
        i.product_code,
        i.quantity,
        i.unit_price,
        i.total_item,
        i.item_status,
        -- Peso proporcional do item no total do pedido (base para rateio do desconto)
        CASE
            WHEN SUM(i.total_item) OVER (PARTITION BY i.order_id) = 0
             AND EXISTS (
                 SELECT 1
                 FROM `{P}.silver.silver_pedidos_cabecalho` c0
                 WHERE c0.order_id = i.order_id
                   AND c0.discount_amount = 0
             )
            THEN 0.0
            ELSE SAFE_DIVIDE(
                i.total_item,
                SUM(i.total_item) OVER (PARTITION BY i.order_id)
            )
        END AS peso_item
    FROM `{P}.silver.silver_pedidos_itens` i
),
fato_base AS (
    SELECT
        -- Surrogate key da fato: order_id e item_seq já são STRING
        TO_HEX(SHA256(CONCAT(c.order_id, '-', i.item_seq))) AS sk_fato,

        -- Surrogate keys de dimensão — todas as chaves são STRING neste ponto
        -- cliente: customer_code (pedidos) = customer_id (clientes) — ambos STRING
        TO_HEX(SHA256(cli.customer_id))    AS sk_cliente,
        -- produto: product_code é STRING — mesma chave usada na dim_produto
        TO_HEX(SHA256(i.product_code))     AS sk_produto,
        -- canal: canal_id vem de silver_vendedores (STRING)
        TO_HEX(SHA256(v.canal_id))         AS sk_canal,
        -- vendedor: seller_id (STRING)
        TO_HEX(SHA256(c.seller_id))        AS sk_vendedor,
        -- data: CAST necessário somente aqui — DATE não é STRING
        TO_HEX(SHA256(CAST(c.order_date AS STRING))) AS sk_data,
        -- regiao: regional_code do vendedor (STRING) → dim_regiao
        TO_HEX(SHA256(v.regional_code))    AS sk_regiao,

        -- Chaves de negócio (para auditoria e joins ad-hoc)
        c.order_id,
        i.item_seq,
        c.customer_code,
        cli.customer_id,
        i.product_code,
        c.seller_id,
        v.canal_id,
        v.regional_code,

        -- Datas
        c.order_date,
        c.promised_date,

        -- Status
        c.status_order,
        i.item_status,

        -- Métricas do item
        i.quantity                    AS quantidade,
        i.unit_price                  AS preco_unitario,
        i.total_item                  AS valor_bruto_item,

        -- Desconto rateado proporcionalmente por item (evita dupla contagem)
        ROUND(COALESCE(c.discount_amount, 0) * i.peso_item, 4) AS desconto_rateado_item,

        -- Receita líquida por item
        ROUND(
            i.total_item - COALESCE(c.discount_amount, 0) * i.peso_item,
            4
        ) AS receita_liquida_item,

        -- Campos de nível pedido (agregar via COUNT DISTINCT order_id)
        c.gross_amount    AS valor_bruto_pedido,
        c.discount_amount AS desconto_pedido,
        c.net_amount      AS valor_liquido_pedido,
        c.payment_details AS forma_pagamento,

        CURRENT_TIMESTAMP() AS _loaded_at

    FROM `{P}.silver.silver_pedidos_cabecalho` c
    INNER JOIN itens_com_peso i
        ON c.order_id = i.order_id
    -- Resolve customer_code → customer_id (ambos STRING após Bronze)
    LEFT JOIN `{P}.silver.silver_clientes` cli
        ON c.customer_code = cli.customer_id
    -- Resolve seller_id → canal_id e regional_code
    LEFT JOIN `{P}.silver.silver_vendedores` v
        ON c.seller_id = v.seller_id
)

SELECT
    f.*,

    CASE
        WHEN NULLIF(TRIM(f.customer_code), '') IS NULL
            THEN 'NAO_INFORMADO'
        WHEN f.customer_id IS NULL
            THEN 'REFERENCIA_INEXISTENTE'
        ELSE 'RESOLVIDO'
    END AS qualidade_cliente,

    CASE
        WHEN NULLIF(TRIM(f.product_code), '') IS NULL
            THEN 'NAO_INFORMADO'
        WHEN p.product_id IS NULL
            THEN 'REFERENCIA_INEXISTENTE'
        ELSE 'RESOLVIDO'
    END AS qualidade_produto,

    CASE
        WHEN NULLIF(TRIM(f.canal_id), '') IS NULL
            THEN 'NAO_INFORMADO'
        WHEN ca.canal_id IS NULL
            THEN 'REFERENCIA_INEXISTENTE'
        ELSE 'RESOLVIDO'
    END AS qualidade_canal,

    CASE
        WHEN NULLIF(TRIM(f.regional_code), '') IS NULL
            THEN 'NAO_INFORMADO'
        WHEN r.regional_code IS NULL
            THEN 'REFERENCIA_INEXISTENTE'
        ELSE 'RESOLVIDO'
    END AS qualidade_regiao

FROM fato_base f

LEFT JOIN (
    SELECT DISTINCT product_id
    FROM `{P}.silver.silver_produtos`
) p
    ON f.product_code = p.product_id

LEFT JOIN (
    SELECT DISTINCT canal_id
    FROM `{P}.silver.silver_canais`
) ca
    ON f.canal_id = ca.canal_id

LEFT JOIN (
    SELECT DISTINCT regional_code
    FROM `{P}.silver.silver_regioes`
) r
    ON f.regional_code = r.regional_code
"""

# ---------------------------------------------------------------------------
# metricas_por_periodo
# ---------------------------------------------------------------------------

SQL_METRICAS_PERIODO = f"""
SELECT
    FORMAT_DATE('%Y-%m', order_date) AS ano_mes,
    EXTRACT(YEAR  FROM order_date)   AS ano,
    EXTRACT(MONTH FROM order_date)   AS mes,
    COUNT(DISTINCT order_id)              AS total_pedidos,
    SUM(quantidade)                       AS total_itens,
    ROUND(SUM(receita_liquida_item),  2)  AS receita_liquida_total,
    ROUND(SUM(valor_bruto_item),      2)  AS valor_bruto_total,
    ROUND(SUM(desconto_rateado_item), 2)  AS desconto_total,
    ROUND(AVG(receita_liquida_item),  2)  AS ticket_medio_item,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.gold.fato_pedidos`
WHERE status_order != 'CANCELADO'
GROUP BY 1, 2, 3
ORDER BY 1
"""

# ---------------------------------------------------------------------------
# metricas_por_regiao
# ---------------------------------------------------------------------------

SQL_METRICAS_REGIAO = f"""
SELECT
    f.regional_code,
    f.qualidade_regiao,
    r.nome_regiao,
    r.estado,
    COUNT(DISTINCT f.order_id)            AS total_pedidos,
    SUM(f.quantidade)                     AS total_itens,
    ROUND(SUM(f.receita_liquida_item), 2) AS receita_liquida_total,
    ROUND(AVG(f.receita_liquida_item), 2) AS ticket_medio_item,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.gold.fato_pedidos` f
LEFT JOIN `{P}.gold.dim_regiao` r ON f.sk_regiao = r.sk_regiao
WHERE f.status_order != 'CANCELADO'
GROUP BY 1, 2, 3, 4
"""

# ---------------------------------------------------------------------------
# metricas_por_canal
# ---------------------------------------------------------------------------

SQL_METRICAS_CANAL = f"""
SELECT
    f.sk_canal,
    f.qualidade_canal,
    c.nome_canal,
    c.tipo_canal,
    COUNT(DISTINCT f.order_id)            AS total_pedidos,
    SUM(f.quantidade)                     AS total_itens,
    ROUND(SUM(f.receita_liquida_item), 2) AS receita_liquida_total,
    ROUND(AVG(f.receita_liquida_item), 2) AS ticket_medio_item,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.gold.fato_pedidos` f
LEFT JOIN `{P}.gold.dim_canal` c ON f.sk_canal = c.sk_canal
WHERE f.status_order != 'CANCELADO'
GROUP BY 1, 2, 3, 4
"""

# ---------------------------------------------------------------------------
# metricas_por_categoria
# ---------------------------------------------------------------------------

SQL_METRICAS_CATEGORIA = f"""
SELECT
    f.qualidade_produto,
    p.categoria,
    p.subcategoria,
    COUNT(DISTINCT f.order_id)            AS total_pedidos,
    SUM(f.quantidade)                     AS total_itens,
    ROUND(SUM(f.receita_liquida_item), 2) AS receita_liquida_total,
    ROUND(AVG(f.preco_unitario),        2) AS preco_medio,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM `{P}.gold.fato_pedidos` f
LEFT JOIN `{P}.gold.dim_produto` p ON f.sk_produto = p.sk_produto
WHERE f.status_order != 'CANCELADO'
GROUP BY 1, 2, 3
"""

# ---------------------------------------------------------------------------
# metricas_operacionais
# Fan-out eliminado: pedidos_base deduplica por order_id;
# ocorrencias_por_pedido agrega antes do join
# ---------------------------------------------------------------------------

SQL_METRICAS_OPERACIONAIS = f"""
WITH pedidos_base AS (
    SELECT DISTINCT
        order_id,
        order_date,
        status_order,
        valor_liquido_pedido AS net_amount
    FROM `{P}.gold.fato_pedidos`
),
ocorrencias_por_pedido AS (
    SELECT
        order_id,
        COUNT(*)                   AS total_ocorrencias,
        COUNTIF(UPPER(TRIM(severity)) IN ('HIGH', 'ALTA')) AS ocorrencias_alta_severidade
    FROM `{P}.silver.silver_ocorrencias`
    GROUP BY order_id
)
SELECT
    FORMAT_DATE('%Y-%m', p.order_date) AS ano_mes,
    p.status_order,
    COUNT(p.order_id)                  AS total_pedidos,

    COUNTIF(e.delivery_status = 'ENTREGUE')    AS pedidos_entregues,
    COUNTIF(UPPER(TRIM(e.delivery_status)) IN ('IN_TRANSIT', 'EM_TRANSITO')) AS pedidos_em_transito,
    COUNTIF(UPPER(TRIM(e.delivery_status)) = 'PENDENTE')    AS pedidos_pendentes,
    COUNTIF(e.delivery_id IS NULL)             AS pedidos_sem_entrega,
    COUNTIF(
        e.delivery_id IS NOT NULL
        AND NULLIF(TRIM(e.delivery_status), '') IS NULL
    ) AS pedidos_entrega_sem_status,
    COUNTIF(UPPER(TRIM(e.delivery_status)) = 'ATRASADO')
        AS pedidos_atrasados,
    COUNTIF(UPPER(TRIM(e.delivery_status)) = 'CANCELADO')
        AS pedidos_entrega_cancelada,

    COUNTIF(o.total_ocorrencias > 0)                AS pedidos_com_ocorrencia,
    COUNTIF(o.ocorrencias_alta_severidade > 0) AS pedidos_com_ocorrencia_alta,
    COALESCE(SUM(o.ocorrencias_alta_severidade), 0) AS ocorrencias_alta_severidade,

    ROUND(AVG(e.freight_cost), 2) AS custo_medio_entrega,

    CURRENT_TIMESTAMP() AS _loaded_at
FROM pedidos_base p
LEFT JOIN `{P}.silver.silver_entregas`   e ON p.order_id = e.order_id
LEFT JOIN ocorrencias_por_pedido         o ON p.order_id = o.order_id
GROUP BY 1, 2
ORDER BY 1, 2
"""

# ---------------------------------------------------------------------------
# vw_kpis_pedidos — ponto único de consumo BI
# Resolve todas as dimensões, filtra cancelados
# ---------------------------------------------------------------------------

SQL_VW_KPIS = f"""
SELECT
    f.sk_fato,
    f.order_id,
    f.item_seq,

    -- Cliente
    c.nome_cliente,
    c.segmento       AS segmento_cliente,
    c.porte          AS porte_cliente,
    c.estado         AS estado_cliente,
    c.cidade         AS cidade_cliente,

    -- Produto
    p.nome_produto,
    p.categoria,
    p.subcategoria,
    p.familia,

    -- Canal
    ca.nome_canal,
    ca.tipo_canal,

    -- Vendedor
    v.nome_vendedor,
    v.codigo_regional,

    -- Região
    r.nome_regiao,
    r.estado         AS estado_regiao,

    -- Data
    d.ano,
    d.trimestre,
    d.mes,
    d.nome_mes,
    d.ano_mes,
    d.semana_ano,
    d.dia,
    d.dia_semana_nome,
    d.fim_de_semana,

    -- Status
    f.status_order,
    f.item_status,

    -- Métricas
    f.quantidade,
    f.preco_unitario,
    f.valor_bruto_item,
    f.desconto_rateado_item,
    f.receita_liquida_item,
    f.forma_pagamento

FROM `{P}.gold.fato_pedidos` f
LEFT JOIN `{P}.gold.dim_cliente`  c  ON f.sk_cliente  = c.sk_cliente
LEFT JOIN `{P}.gold.dim_produto`  p  ON f.sk_produto  = p.sk_produto
LEFT JOIN `{P}.gold.dim_canal`    ca ON f.sk_canal    = ca.sk_canal
LEFT JOIN `{P}.gold.dim_vendedor` v  ON f.sk_vendedor = v.sk_vendedor
LEFT JOIN `{P}.gold.dim_regiao`   r  ON f.sk_regiao   = r.sk_regiao
LEFT JOIN `{P}.gold.dim_data`     d  ON f.sk_data     = d.sk_data

WHERE f.status_order != 'CANCELADO'
"""

# ---------------------------------------------------------------------------
# Pipeline GOLD
# ---------------------------------------------------------------------------

def run_gold_model():
    # Pre-flight bloqueante antes de qualquer escrita Gold.
    from importlib.util import module_from_spec, spec_from_file_location

    preflight_path = Path(__file__).with_name("07_gold_preflight.py")

    if not preflight_path.is_file():
        raise RuntimeError("Gold bloqueada: pre-flight ausente.")

    spec = spec_from_file_location("gold_preflight", preflight_path)

    if spec is None or spec.loader is None:
        raise RuntimeError("Gold bloqueada: pre-flight indisponivel.")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_gold_preflight()

    bq = get_bq_client()
    ensure_ingestion_log(bq)
    logger.info(f"=== GOLD Model | execution_id={EXECUTION_ID} ===")

    dim_steps = [
        ("gold.dim_cliente",  SQL_DIM_CLIENTE,  "dim_cliente"),
        ("gold.dim_produto",  SQL_DIM_PRODUTO,  "dim_produto"),
        ("gold.dim_canal",    SQL_DIM_CANAL,    "dim_canal"),
        ("gold.dim_regiao",   SQL_DIM_REGIAO,   "dim_regiao"),
        ("gold.dim_vendedor", SQL_DIM_VENDEDOR, "dim_vendedor"),
        ("gold.dim_data",     SQL_DIM_DATA,     "dim_data"),
    ]

    for table, sql, label in dim_steps:
        with StepLogger(bq, EXECUTION_ID, table, LAYER) as step:
            step.rows = create_or_replace(bq, table, sql, description=label)

    with StepLogger(bq, EXECUTION_ID, "gold.fato_pedidos", LAYER) as step:
        step.rows = create_or_replace(bq, "gold.fato_pedidos", SQL_FATO_PEDIDOS, "fato_pedidos")

    metricas_steps = [
        ("gold.metricas_por_periodo",   SQL_METRICAS_PERIODO,     "metricas_por_periodo"),
        ("gold.metricas_por_regiao",    SQL_METRICAS_REGIAO,      "metricas_por_regiao"),
        ("gold.metricas_por_canal",     SQL_METRICAS_CANAL,       "metricas_por_canal"),
        ("gold.metricas_por_categoria", SQL_METRICAS_CATEGORIA,   "metricas_por_categoria"),
        ("gold.metricas_operacionais",  SQL_METRICAS_OPERACIONAIS,"metricas_operacionais"),
    ]

    for table, sql, label in metricas_steps:
        with StepLogger(bq, EXECUTION_ID, table, LAYER) as step:
            step.rows = create_or_replace(bq, table, sql, description=label)

    create_or_replace_view(bq, "gold.vw_kpis_pedidos", SQL_VW_KPIS, "vw_kpis_pedidos")
    logger.info("[OK] gold.vw_kpis_pedidos — view criada")

    logger.info("=== GOLD Model concluído ===")
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
    run_gold_model()
