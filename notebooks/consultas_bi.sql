-- =============================================================================
-- consultas_bi.sql
-- Consultas analíticas prontas para consumo sobre gold.vw_kpis_pedidos
-- Projeto: eli-lakehouse-gcp
-- =============================================================================


-- 1. Receita líquida por mês (série temporal)
SELECT
    ano_mes,
    COUNT(DISTINCT order_id)         AS total_pedidos,
    SUM(quantidade)                  AS total_itens,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida,
    ROUND(SUM(desconto_rateado_item), 2) AS desconto_concedido
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1
ORDER BY 1;


-- 2. Receita por região e estado
SELECT
    nome_regiao,
    estado_regiao,
    COUNT(DISTINCT order_id)            AS total_pedidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida,
    ROUND(AVG(receita_liquida_item), 2) AS ticket_medio
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1, 2
ORDER BY receita_liquida DESC;


-- 3. Top 10 produtos por receita
SELECT
    nome_produto,
    categoria,
    SUM(quantidade)                     AS unidades_vendidas,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida,
    ROUND(AVG(preco_unitario), 2)       AS preco_medio
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1, 2
ORDER BY receita_liquida DESC
LIMIT 10;


-- 4. Desempenho por canal de venda
SELECT
    nome_canal,
    tipo_canal,
    COUNT(DISTINCT order_id)            AS total_pedidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida,
    ROUND(AVG(receita_liquida_item), 2) AS ticket_medio_item
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1, 2
ORDER BY receita_liquida DESC;


-- 5. Ranking de vendedores
SELECT
    nome_vendedor,
    codigo_regional,
    COUNT(DISTINCT order_id)            AS total_pedidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1, 2
ORDER BY receita_liquida DESC;


-- 6. Receita por segmento de cliente
SELECT
    segmento_cliente,
    COUNT(DISTINCT order_id)            AS total_pedidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida,
    ROUND(AVG(receita_liquida_item), 2) AS ticket_medio
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1
ORDER BY receita_liquida DESC;


-- 7. Receita por categoria × mês (pivot analítico)
SELECT
    ano_mes,
    categoria,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1, 2
ORDER BY 1, receita_liquida DESC;


-- 8. KPIs executivos (single-row summary)
SELECT
    COUNT(DISTINCT order_id)            AS total_pedidos,
    COUNT(*)                            AS total_linhas_fato,
    SUM(quantidade)                     AS total_itens_vendidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida_total,
    ROUND(SUM(desconto_rateado_item), 2) AS desconto_total,
    ROUND(AVG(receita_liquida_item), 2)  AS ticket_medio_item,
    MIN(ano_mes)                         AS primeiro_periodo,
    MAX(ano_mes)                         AS ultimo_periodo
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`;


-- 9. Dias da semana com maior volume
SELECT
    dia_semana_nome,
    fim_de_semana,
    COUNT(DISTINCT order_id)            AS total_pedidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_liquida
FROM `eli-lakehouse-gcp.gold.vw_kpis_pedidos`
GROUP BY 1, 2
ORDER BY total_pedidos DESC;


-- 10. Auditoria: pedidos com cancelamento (fora da view, consulta direto na fato)
SELECT
    status_order,
    COUNT(DISTINCT order_id) AS total_pedidos,
    ROUND(SUM(receita_liquida_item), 2) AS receita_potencial_perdida
FROM `eli-lakehouse-gcp.gold.fato_pedidos`
WHERE status_order = 'CANCELADO'
GROUP BY 1;
