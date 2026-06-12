# Lakehouse Case — Implementação GCP

Implementação fiel do case técnico de Engenharia de Dados no Google Cloud Platform.
Arquitetura: Cloud Storage → BigQuery RAW → BRONZE → SILVER → GOLD.

---

## Pré-requisitos

```bash
pip install google-cloud-bigquery google-cloud-storage pandas openpyxl db-dtypes
```

Autenticação:
```bash
gcloud auth application-default login
# ou: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

---

## Estrutura

```
notebooks/
├── 00_setup.py              # Config central, helpers, logging
├── 01_raw_ingestion.py      # GCS → RAW (9 tabelas, fidelidade total)
├── 02_bronze_transform.py   # RAW → BRONZE (correções técnicas)
├── 03_silver_integrate.py   # BRONZE → SILVER (integração, dedup, órfãos)
├── 04_gold_model.py         # SILVER → GOLD (dimensional + métricas + view)
├── 05_quality_checks.py     # Validações cross-layer → quality.validation_results
├── consultas_bi.sql         # 10 consultas analíticas sobre vw_kpis_pedidos
├── run_pipeline.py          # Orquestrador de execução sequencial
└── setup_00.py              # Alias de importação para 00_setup.py
```

---

## Execução

### Pipeline completa
```bash
cd notebooks/
python run_pipeline.py
```

### Camada específica (reprocessamento)
```bash
python run_pipeline.py --layer raw
python run_pipeline.py --layer bronze
python run_pipeline.py --layer silver
python run_pipeline.py --layer gold
python run_pipeline.py --layer quality
```

### Notebook individual
```bash
python 01_raw_ingestion.py
python 02_bronze_transform.py
# ...
```

---

## Datasets BigQuery

| Dataset   | Conteúdo                                              |
|-----------|-------------------------------------------------------|
| raw       | 9 tabelas — dados brutos, tudo STRING                 |
| bronze    | 9 tabelas — tipos corretos, correções técnicas        |
| silver    | Entidades conformadas + tabelas _orphan               |
| gold      | Dimensões, fato, métricas, vw_kpis_pedidos            |
| quality   | quality.validation_results                            |
| control   | control.ingestion_log                                 |

---

## Tabelas produzidas

### RAW
- raw.erp_pedidos_cabecalho
- raw.erp_pedidos_itens
- raw.crm_clientes
- raw.comercial_canais
- raw.vendedores
- raw.regioes
- raw.produtos
- raw.logistica_entregas
- raw.atendimento_ocorrencias

### BRONZE
- bronze.erp_pedidos_cabecalho
- bronze.erp_pedidos_itens
- bronze.crm_clientes
- bronze.comercial_canais
- bronze.vendedores
- bronze.regioes
- bronze.produtos
- bronze.logistica_entregas
- bronze.atendimento_ocorrencias

### SILVER
- silver.silver_pedidos_cabecalho
- silver.silver_pedidos_itens
- silver.silver_clientes
- silver.silver_canais
- silver.silver_vendedores
- silver.silver_regioes
- silver.silver_produtos
- silver.silver_entregas
- silver.silver_ocorrencias
- silver.silver_ocorrencias_orphan
- silver.silver_pedidos_itens_orphan

### GOLD
- gold.dim_cliente
- gold.dim_produto
- gold.dim_canal
- gold.dim_regiao
- gold.dim_vendedor
- gold.dim_data
- gold.fato_pedidos
- gold.metricas_por_periodo
- gold.metricas_por_regiao
- gold.metricas_por_canal
- gold.metricas_por_categoria
- gold.metricas_operacionais
- gold.vw_kpis_pedidos (VIEW)

### CONTROL
- control.ingestion_log (particionada por dia)

### QUALITY
- quality.validation_results (particionada por dia)

---

## Decisões técnicas preservadas do case original

| Decisão | Implementação GCP |
|---------|-------------------|
| Surrogate keys via sha2 | `TO_HEX(SHA256(...))` no BigQuery |
| split cabecalho/itens | join ocorre na fato_pedidos (Gold) |
| receita_liquida_item rateada | `peso_item = total_item / SUM(total_item) OVER (PARTITION BY order_id)` |
| dim_data dinâmica | `GENERATE_DATE_ARRAY` do range efetivo ±30 dias |
| xlsx via pandas | `pd.read_excel()` com `dtype=str` no RAW |
| cancelados filtrados na view | `WHERE status_order != 'CANCELADO'` apenas em `vw_kpis_pedidos` |
| entrega mais recente por pedido | `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_at DESC)` |
| SE duplicada → SP | `ORDER BY CASE WHEN state = 'SP' THEN 0 ELSE 1 END` na dedup |
| XX descartado | `WHERE region_code_clean IS NOT NULL` no Bronze |

---

## Idempotência

Todos os notebooks usam `CREATE OR REPLACE TABLE` (BigQuery DDL).
Reexecuções são seguras — não acumulam dados duplicados.
O RAW usa `WRITE_TRUNCATE` na carga via pandas.
`control.ingestion_log` e `quality.validation_results` são append-only com `execution_id` por execução.
