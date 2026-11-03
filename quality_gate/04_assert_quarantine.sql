-- Bloqueio de publicacao: consistencia da quarentena.
-- Falha se houver divergencia entre classificador e quarentena.

ASSERT (
    SELECT
        rejeitados_classificador = registros_quarentena
        AND rejeitados_nao_persistidos = 0
        AND registros_obsoletos = 0
    FROM (
WITH classified AS (
-- Quality Gate: classificacao de pedidos
-- Projeto: gcp-lakehouse-case
-- Fonte: bronze.erp_pedidos_cabecalho
-- Operacao somente de leitura

WITH validated AS (
    SELECT
        b.*,

        ARRAY_CONCAT(
            IF(
                order_id IS NULL OR TRIM(order_id) = '',
                ['DQ001_ORDER_ID_REQUIRED'],
                ARRAY<STRING>[]
            ),
            IF(
                order_date IS NULL,
                ['DQ002_ORDER_DATE_INVALID'],
                ARRAY<STRING>[]
            ),
            IF(
                gross_amount IS NULL OR gross_amount < 0,
                ['DQ003_GROSS_AMOUNT_INVALID'],
                ARRAY<STRING>[]
            ),
            IF(
                discount_amount IS NULL
                OR gross_amount IS NULL
                OR discount_amount < 0
                OR discount_amount > gross_amount,
                ['DQ004_DISCOUNT_INVALID'],
                ARRAY<STRING>[]
            ),
            IF(
                net_amount IS NULL
                OR gross_amount IS NULL
                OR discount_amount IS NULL
                OR ABS(
                    net_amount - (gross_amount - discount_amount)
                ) > 0.01,
                ['DQ005_NET_AMOUNT_INVALID'],
                ARRAY<STRING>[]
            )
        ) AS quality_errors

    FROM `eli-lakehouse-gcp.bronze.erp_pedidos_cabecalho` b
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                IF(ARRAY_LENGTH(quality_errors) = 0, 0, 1),
                source_updated_at DESC,
                TO_JSON_STRING(STRUCT(
                    customer_code,
                    seller_id,
                    order_date,
                    promised_date,
                    status_order,
                    gross_amount,
                    discount_amount,
                    net_amount,
                    payment_details
                )) DESC
        ) AS selection_rank

    FROM validated
),

classified AS (
    SELECT
        *,
        CASE
            WHEN ARRAY_LENGTH(quality_errors) > 0
                THEN 'QUARANTINED'
            WHEN selection_rank = 1
                THEN 'APPROVED'
            ELSE 'SUPERSEDED'
        END AS quality_status

    FROM ranked
)

SELECT
    *
FROM classified
),
expected_quarantine AS (
    SELECT
        TO_HEX(SHA256(TO_JSON_STRING(
            (SELECT AS STRUCT c.* EXCEPT(
                _loaded_at,
                quality_errors,
                selection_rank,
                quality_status
            ))
        ))) AS record_hash
    FROM classified c
    WHERE quality_status = 'QUARANTINED'
),
actual_quarantine AS (
    SELECT record_hash
    FROM `eli-lakehouse-gcp.quality.quarantine_records`
)
SELECT
    (SELECT COUNT(*) FROM expected_quarantine)
        AS rejeitados_classificador,
    (SELECT COUNT(*) FROM actual_quarantine)
        AS registros_quarentena,
    (
        SELECT COUNT(*)
        FROM expected_quarantine e
        LEFT JOIN actual_quarantine a USING (record_hash)
        WHERE a.record_hash IS NULL
    ) AS rejeitados_nao_persistidos,
    (
        SELECT COUNT(*)
        FROM actual_quarantine a
        LEFT JOIN expected_quarantine e USING (record_hash)
        WHERE e.record_hash IS NULL
    ) AS registros_obsoletos
    )
) AS 'QUALITY_GATE_FAILED: quarentena divergente do classificador';
