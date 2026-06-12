# =============================================================================
# 02_bronze_transform.py
# Camada BRONZE — Correções técnicas sobre o RAW
#
# Responsabilidades (apenas):
#   - Padronização de tipos
#   - Parse de datas (múltiplos formatos)
#   - Normalização de texto
#   - Tratamento de nulos
#   - Recomputação de campos inconsistentes (total_item)
#   - Normalização de códigos (delivery_status, regional_code, estado)
#
# Sem regras de negócio. Sem deduplicação. Sem joins de integração.
#
# Schema source: schemas reais confirmados pela execução do RAW em produção
# =============================================================================

import logging
from google.cloud import bigquery

from setup_00 import (
    PROJECT_ID,
    get_bq_client, ensure_ingestion_log, new_execution_id,
    run_query, StepLogger, logger,
)

EXECUTION_ID = new_execution_id()
LAYER = "BRONZE"


# ---------------------------------------------------------------------------
# Helpers de DDL
# ---------------------------------------------------------------------------

def create_or_replace(bq: bigquery.Client, table: str, select_sql: str, description: str):
    sql = f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{table}` AS\n{select_sql}"
    run_query(bq, sql, description)
    row_count = bq.query(f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.{table}`").to_dataframe()["n"][0]
    return int(row_count)


# ---------------------------------------------------------------------------
# BRONZE: erp_pedidos_cabecalho
# RAW real: order_id, customer_code, seller_id, order_date, promised_date,
#           status_order, gross_amount, discount_amount, net_amount,
#           payment_details, last_update
# Correções: status_order NULL → DESCONHECIDO, 3 formatos de data, tipos
# Nota: channel e region não existem nesta fonte — resolvidos via vendedores
# ---------------------------------------------------------------------------

SQL_BRONZE_PEDIDOS_CAB = """
SELECT
    UPPER(TRIM(order_id))                AS order_id,
    UPPER(TRIM(customer_code))           AS customer_code,
    UPPER(TRIM(seller_id))               AS seller_id,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', order_date),
        SAFE.PARSE_DATE('%d/%m/%Y', order_date),
        SAFE.PARSE_DATE('%d-%m-%Y', order_date)
    ) AS order_date,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', promised_date),
        SAFE.PARSE_DATE('%d/%m/%Y', promised_date)
    ) AS promised_date,

    -- status_order nulo → DESCONHECIDO
    COALESCE(NULLIF(TRIM(status_order), ''), 'DESCONHECIDO') AS status_order,

    SAFE_CAST(REPLACE(gross_amount,    ',', '.') AS FLOAT64) AS gross_amount,
    SAFE_CAST(REPLACE(discount_amount, ',', '.') AS FLOAT64) AS discount_amount,
    SAFE_CAST(REPLACE(net_amount,      ',', '.') AS FLOAT64) AS net_amount,

    TRIM(payment_details) AS payment_details,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM `{project}.raw.erp_pedidos_cabecalho`
"""

# ---------------------------------------------------------------------------
# BRONZE: erp_pedidos_itens
# RAW real: order_id, item_seq, product_code, quantity, unit_price,
#           total_item, item_status
# Correções: item_status NULL → NAO_INFORMADO, recomputação de total_item
# Nota: category e product_name não existem nesta fonte — resolvidos via produtos
# ---------------------------------------------------------------------------

SQL_BRONZE_PEDIDOS_ITENS = """
SELECT
    UPPER(TRIM(order_id))   AS order_id,
    UPPER(TRIM(item_seq))   AS item_seq,
    UPPER(TRIM(product_code)) AS product_code,
    CAST(SAFE_CAST(REPLACE(quantity, ',', '.') AS FLOAT64) AS INT64) AS quantity,

    SAFE_CAST(REPLACE(unit_price, ',', '.') AS FLOAT64) AS unit_price,
    SAFE_CAST(REPLACE(total_item, ',', '.') AS FLOAT64) AS total_item_original,

    -- Recomputa total_item; tolerância 0.01 conforme README
    ROUND(
        CAST(SAFE_CAST(REPLACE(quantity, ',', '.') AS FLOAT64) AS INT64) *
        SAFE_CAST(REPLACE(unit_price, ',', '.') AS FLOAT64),
        2
    ) AS total_item,

    -- item_status nulo (26%) → NAO_INFORMADO
    COALESCE(NULLIF(TRIM(item_status), ''), 'NAO_INFORMADO') AS item_status,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM `{project}.raw.erp_pedidos_itens`
"""

# ---------------------------------------------------------------------------
# BRONZE: crm_clientes
# RAW real: customer_id, nome_cliente, segmento, porte, cidade, estado,
#           data_cadastro, email, status_cliente, updated_at
# Correções: estado com nomes completos e siglas → normalização
# ---------------------------------------------------------------------------

SQL_BRONZE_CLIENTES = """
SELECT
    UPPER(TRIM(customer_id))          AS customer_id,
    TRIM(nome_cliente)                AS nome_cliente,
    TRIM(email)                       AS email,
    TRIM(segmento)                    AS segmento,
    TRIM(porte)                       AS porte,
    TRIM(cidade)                      AS cidade,
    TRIM(status_cliente)              AS status_cliente,

    -- Normaliza estado: nomes completos → siglas de 2 letras
    CASE
        WHEN UPPER(TRIM(estado)) IN ('SP', 'SÃO PAULO', 'SAO PAULO')   THEN 'SP'
        WHEN UPPER(TRIM(estado)) IN ('RJ', 'RIO DE JANEIRO')            THEN 'RJ'
        WHEN UPPER(TRIM(estado)) IN ('MG', 'MINAS GERAIS')              THEN 'MG'
        WHEN UPPER(TRIM(estado)) IN ('RS', 'RIO GRANDE DO SUL')         THEN 'RS'
        WHEN UPPER(TRIM(estado)) IN ('PR', 'PARANÁ', 'PARANA')          THEN 'PR'
        WHEN UPPER(TRIM(estado)) IN ('SC', 'SANTA CATARINA')            THEN 'SC'
        WHEN UPPER(TRIM(estado)) IN ('BA', 'BAHIA')                     THEN 'BA'
        WHEN UPPER(TRIM(estado)) IN ('GO', 'GOIÁS', 'GOIAS')            THEN 'GO'
        WHEN UPPER(TRIM(estado)) IN ('PE', 'PERNAMBUCO')                THEN 'PE'
        WHEN UPPER(TRIM(estado)) IN ('CE', 'CEARÁ', 'CEARA')            THEN 'CE'
        WHEN UPPER(TRIM(estado)) IN ('AM', 'AMAZONAS')                  THEN 'AM'
        WHEN UPPER(TRIM(estado)) IN ('PA', 'PARÁ', 'PARA')              THEN 'PA'
        WHEN UPPER(TRIM(estado)) IN ('MT', 'MATO GROSSO')               THEN 'MT'
        WHEN UPPER(TRIM(estado)) IN ('MS', 'MATO GROSSO DO SUL')        THEN 'MS'
        WHEN UPPER(TRIM(estado)) IN ('DF', 'DISTRITO FEDERAL')          THEN 'DF'
        WHEN UPPER(TRIM(estado)) IN ('ES', 'ESPÍRITO SANTO','ESPIRITO SANTO') THEN 'ES'
        WHEN UPPER(TRIM(estado)) IN ('RN', 'RIO GRANDE DO NORTE')       THEN 'RN'
        WHEN UPPER(TRIM(estado)) IN ('PB', 'PARAÍBA', 'PARAIBA')        THEN 'PB'
        WHEN UPPER(TRIM(estado)) IN ('AL', 'ALAGOAS')                   THEN 'AL'
        WHEN UPPER(TRIM(estado)) IN ('SE', 'SERGIPE')                   THEN 'SE'
        WHEN UPPER(TRIM(estado)) IN ('PI', 'PIAUÍ', 'PIAUI')            THEN 'PI'
        WHEN UPPER(TRIM(estado)) IN ('MA', 'MARANHÃO', 'MARANHAO')      THEN 'MA'
        WHEN UPPER(TRIM(estado)) IN ('TO', 'TOCANTINS')                 THEN 'TO'
        WHEN UPPER(TRIM(estado)) IN ('RO', 'RONDÔNIA', 'RONDONIA')      THEN 'RO'
        WHEN UPPER(TRIM(estado)) IN ('AC', 'ACRE')                      THEN 'AC'
        WHEN UPPER(TRIM(estado)) IN ('RR', 'RORAIMA')                   THEN 'RR'
        WHEN UPPER(TRIM(estado)) IN ('AP', 'AMAPÁ', 'AMAPA')            THEN 'AP'
        ELSE UPPER(TRIM(estado))
    END AS estado,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', data_cadastro),
        SAFE.PARSE_DATE('%d/%m/%Y', data_cadastro)
    ) AS data_cadastro,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM `{project}.raw.crm_clientes`
"""

# ---------------------------------------------------------------------------
# BRONZE: comercial_canais
# RAW real: id_canal, nome_canal, tipo_canal, ativo, observacao
# ---------------------------------------------------------------------------

SQL_BRONZE_CANAIS = """
SELECT
    UPPER(TRIM(id_canal))           AS canal_id,
    TRIM(nome_canal)                AS nome_canal,
    TRIM(tipo_canal)                AS tipo_canal,
    TRIM(ativo)                     AS ativo,
    TRIM(observacao)                AS observacao,
    CURRENT_TIMESTAMP()             AS _loaded_at
FROM `{project}.raw.comercial_canais`
"""

# ---------------------------------------------------------------------------
# BRONZE: vendedores
# RAW real: seller_id, seller_name, canal_id, regional_code, hire_date, status
# Correções: regional_code 'sul' → 'S'
# ---------------------------------------------------------------------------

SQL_BRONZE_VENDEDORES = """
SELECT
    UPPER(TRIM(seller_id))          AS seller_id,
    TRIM(seller_name)               AS seller_name,
    UPPER(TRIM(canal_id))           AS canal_id,

    -- regional_code: normaliza alias de texto livre → código de 2 letras
    CASE
        WHEN UPPER(TRIM(regional_code)) IN ('SUL', 'S')                      THEN 'S'
        WHEN UPPER(TRIM(regional_code)) IN ('NORTE', 'N')                    THEN 'N'
        WHEN UPPER(TRIM(regional_code)) IN ('NORDESTE', 'NE')                THEN 'NE'
        WHEN UPPER(TRIM(regional_code)) IN ('SUDESTE', 'SE')                 THEN 'SE'
        WHEN UPPER(TRIM(regional_code)) IN ('CENTRO-OESTE','CENTRO OESTE','CO') THEN 'CO'
        ELSE UPPER(TRIM(regional_code))
    END AS regional_code,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', hire_date),
        SAFE.PARSE_DATE('%d/%m/%Y', hire_date)
    ) AS hire_date,

    TRIM(status) AS status,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM `{project}.raw.vendedores`
"""

# ---------------------------------------------------------------------------
# BRONZE: regioes
# RAW real: regional_code, regional_name, state, manager_name, active_flag
# Correções: código XX descartado
# ---------------------------------------------------------------------------

SQL_BRONZE_REGIOES = """
SELECT
    TRIM(regional_code)             AS regional_code,
    TRIM(regional_name)             AS regional_name,
    TRIM(state)                     AS state,
    TRIM(manager_name)              AS manager_name,
    SAFE_CAST(active_flag AS INT64) AS active_flag,
    CURRENT_TIMESTAMP()             AS _loaded_at

FROM `{project}.raw.regioes`
WHERE UPPER(TRIM(regional_code)) != 'XX'
"""

# ---------------------------------------------------------------------------
# BRONZE: produtos
# RAW real (após sanitize_columns): product_product_id, product_name,
#   product_category, product_subcategory, product_status, pricing_list_price,
#   pricing_currency, attributes_family, attributes_tags, updated_at
# ---------------------------------------------------------------------------

SQL_BRONZE_PRODUTOS = """
SELECT
    TRIM(product_product_id)                                        AS product_id,
    TRIM(product_name)                                              AS product_name,
    TRIM(product_category)                                          AS category,
    TRIM(product_subcategory)                                       AS subcategory,
    TRIM(product_status)                                            AS product_status,
    SAFE_CAST(REPLACE(pricing_list_price, ',', '.') AS FLOAT64)     AS list_price,
    TRIM(pricing_currency)                                          AS currency,
    TRIM(attributes_family)                                         AS family,
    TRIM(attributes_tags)                                           AS tags,
    CURRENT_TIMESTAMP()                                             AS _loaded_at
FROM `{project}.raw.produtos`
"""

# ---------------------------------------------------------------------------
# BRONZE: logistica_entregas
# RAW real (após sanitize_columns): delivery_id, order_ref, delivery_status,
#   cost, carrier_name, carrier_mode, timestamps_shipped_at,
#   timestamps_delivered_at, destination_state, destination_city
# Correções: delivery_status PT/EN mix → mapeamento explícito
# ---------------------------------------------------------------------------

SQL_BRONZE_ENTREGAS = """
SELECT
    UPPER(TRIM(delivery_id))        AS delivery_id,
    UPPER(TRIM(order_ref))          AS order_ref,

    -- Normaliza delivery_status: mix PT/EN → padrão PT
    CASE
        WHEN LOWER(TRIM(delivery_status)) IN ('entregue',    'delivered')  THEN 'ENTREGUE'
        WHEN LOWER(TRIM(delivery_status)) IN ('em trânsito', 'in transit',
                                              'em transito')               THEN 'EM_TRANSITO'
        WHEN LOWER(TRIM(delivery_status)) IN ('pendente',    'pending')    THEN 'PENDENTE'
        WHEN LOWER(TRIM(delivery_status)) IN ('cancelado',   'cancelled',
                                              'canceled')                  THEN 'CANCELADO'
        WHEN LOWER(TRIM(delivery_status)) IN ('devolvido',   'returned')   THEN 'DEVOLVIDO'
        ELSE UPPER(TRIM(delivery_status))
    END AS delivery_status,

    SAFE_CAST(REPLACE(cost, ',', '.') AS FLOAT64) AS cost,

    TRIM(carrier_name) AS carrier_name,
    TRIM(carrier_mode) AS carrier_mode,
    TRIM(destination_state) AS destination_state,
    TRIM(destination_city)  AS destination_city,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', timestamps_shipped_at),
        SAFE.PARSE_DATE('%d/%m/%Y', timestamps_shipped_at),
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', timestamps_shipped_at)),
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  timestamps_shipped_at))
    ) AS shipped_at,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', timestamps_delivered_at),
        SAFE.PARSE_DATE('%d/%m/%Y', timestamps_delivered_at),
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', timestamps_delivered_at)),
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  timestamps_delivered_at))
    ) AS delivered_at,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM `{project}.raw.logistica_entregas`
"""

# ---------------------------------------------------------------------------
# BRONZE: atendimento_ocorrencias
# RAW real: ticket_id, order_id, event_type, created_at, severity, status,
#           metadata_channel, metadata_sla_hours, customer_code
# Correções: event_type e severity nulos → NAO_INFORMADO
# ---------------------------------------------------------------------------

SQL_BRONZE_OCORRENCIAS = """
SELECT
    UPPER(TRIM(ticket_id))          AS ticket_id,
    UPPER(TRIM(order_id))           AS order_id,
    UPPER(TRIM(customer_code))      AS customer_code,

    COALESCE(NULLIF(TRIM(event_type), ''), 'NAO_INFORMADO') AS event_type,
    COALESCE(NULLIF(TRIM(severity),   ''), 'NAO_INFORMADO') AS severity,
    COALESCE(NULLIF(TRIM(status),     ''), 'NAO_INFORMADO') AS status,

    TRIM(metadata_channel)                        AS metadata_channel,
    SAFE_CAST(metadata_sla_hours AS FLOAT64)      AS metadata_sla_hours,

    COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', created_at),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  created_at),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  created_at)
    ) AS created_at,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM `{project}.raw.atendimento_ocorrencias`
"""

# ---------------------------------------------------------------------------
# Pipeline BRONZE
# ---------------------------------------------------------------------------

BRONZE_STEPS = [
    ("bronze.erp_pedidos_cabecalho",   SQL_BRONZE_PEDIDOS_CAB,  "erp_pedidos_cabecalho"),
    ("bronze.erp_pedidos_itens",       SQL_BRONZE_PEDIDOS_ITENS,"erp_pedidos_itens"),
    ("bronze.crm_clientes",            SQL_BRONZE_CLIENTES,     "crm_clientes"),
    ("bronze.comercial_canais",        SQL_BRONZE_CANAIS,       "comercial_canais"),
    ("bronze.vendedores",              SQL_BRONZE_VENDEDORES,   "vendedores"),
    ("bronze.regioes",                 SQL_BRONZE_REGIOES,      "regioes"),
    ("bronze.produtos",                SQL_BRONZE_PRODUTOS,     "produtos"),
    ("bronze.logistica_entregas",      SQL_BRONZE_ENTREGAS,     "logistica_entregas"),
    ("bronze.atendimento_ocorrencias", SQL_BRONZE_OCORRENCIAS,  "atendimento_ocorrencias"),
]


def run_bronze_transform():
    bq = get_bq_client()
    ensure_ingestion_log(bq)
    logger.info(f"=== BRONZE Transform | execution_id={EXECUTION_ID} ===")

    for table, sql_template, label in BRONZE_STEPS:
        sql = sql_template.format(project=PROJECT_ID)
        with StepLogger(bq, EXECUTION_ID, table, LAYER) as step:
            step.rows = create_or_replace(bq, table, sql, description=label)

    logger.info("=== BRONZE Transform concluída ===")
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
    run_bronze_transform()
