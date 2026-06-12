# GCP Lakehouse Case

## Sobre o Projeto

Este projeto foi desenvolvido para demonstrar a implementação de uma arquitetura Lakehouse moderna utilizando serviços nativos da Google Cloud Platform (GCP).

O objetivo foi construir um pipeline completo de dados, desde a ingestão de múltiplas fontes até a disponibilização de informações analíticas para consumo de negócio, aplicando conceitos de modelagem dimensional, qualidade de dados, governança e observabilidade.

A solução simula um ambiente corporativo onde dados provenientes de CRM, ERP, logística, atendimento ao cliente, canais comerciais, produtos, vendedores e sistemas legados precisam ser integrados em uma plataforma única de dados.

---

## Arquitetura da Solução

A arquitetura foi implementada seguindo o padrão Medallion Architecture.

```text
Sistemas de Origem
        │
        ▼
Cloud Storage (RAW)
        │
        ▼
BigQuery RAW
        │
        ▼
BRONZE
        │
        ▼
SILVER
        │
        ▼
Dataform
        │
        ▼
GOLD
        │
        ├── Dimensões
        ├── Fato
        └── Data Marts
        │
        ▼
Quality Layer
        │
        ▼
Consumo Analítico
```

---

## Tecnologias Utilizadas

* Google Cloud Platform (GCP)
* Cloud Storage
* BigQuery
* Dataform
* SQL
* Python
* Modelagem Dimensional
* Data Quality
* Data Governance

---

## Sistemas de Origem

O projeto utiliza múltiplas fontes de dados simulando um ambiente corporativo real.

| Sistema               | Formato |
| --------------------- | ------- |
| CRM Clientes          | XLSX    |
| ERP Pedidos Cabeçalho | CSV     |
| ERP Pedidos Itens     | CSV     |
| Canais Comerciais     | XLSX    |
| Produtos              | JSON    |
| Entregas              | JSON    |
| Atendimento           | NDJSON  |
| Vendedores            | CSV     |
| Regiões Legadas       | TXT     |

---

## Estrutura do Cloud Storage

```text
eli-lakehouse-gcp-lake

├── raw
├── bronze
├── silver
├── gold
├── quality
├── control
├── jobs
└── evidence
```

### RAW

Armazena os arquivos exatamente como recebidos dos sistemas de origem.

### BRONZE

Primeira camada de ingestão e padronização.

### SILVER

Camada responsável pela integração, limpeza e enriquecimento dos dados.

### GOLD

Camada analítica contendo dimensões, fato e marts.

### QUALITY

Camada responsável pelo armazenamento dos resultados das validações.

### CONTROL

Camada responsável pelo armazenamento de logs e informações operacionais.

---

## Datasets BigQuery

Foram criados os seguintes datasets:

```text
raw
bronze
silver
gold
quality
control
```

---

## Modelagem Dimensional

A camada GOLD foi modelada utilizando Star Schema.

### Dimensões

* dim_cliente
* dim_produto
* dim_vendedor
* dim_canal
* dim_regiao
* dim_data

### Fato

* fato_pedidos

A tabela fato centraliza os indicadores de negócio e se relaciona com as dimensões por meio de surrogate keys.

---

## Data Marts

Foram criados marts analíticos para consumo de negócio.

### metricas_por_regiao

Consolida indicadores de pedidos, volume e receita por região.

### metricas_por_canal

Apresenta métricas agregadas por canal de vendas.

### metricas_por_categoria

Permite análise de receita, volume e comportamento por categoria e subcategoria de produtos.

---

## Qualidade dos Dados

Foi implementado um framework centralizado de Data Quality através da tabela:

quality.validation_results

Validações implementadas:

* Row Count
* Duplicate Check
* Null Check
* Referential Integrity
* Orphan Check
* Customer Resolution
* Discount Consistency

Cada execução registra:

* Execution ID
* Camada
* Tabela
* Tipo de Validação
* Métrica
* Resultado

---

## Governança e Observabilidade

A camada de controle registra informações operacionais do pipeline através da tabela:

control.ingestion_log

Esses registros permitem acompanhar execuções, volumes processados e comportamento dos processos ao longo do tempo.

---

## Orquestração com Dataform

O Dataform foi utilizado para organizar as transformações, controlar dependências e disponibilizar linhagem entre os objetos analíticos.

Objetos implementados:

### Dimensões

* dim_cliente
* dim_produto
* dim_vendedor

### Fato

* fato_pedidos

### Data Marts

* metricas_por_regiao
* metricas_por_canal
* metricas_por_categoria

Além da execução automatizada, o projeto possui grafo de dependências e linhagem visual através do Compiled Graph.

---

## Conceitos Demonstrados

* Lakehouse Architecture
* Medallion Architecture
* Cloud Storage
* BigQuery
* Dataform
* Modelagem Dimensional
* Star Schema
* Data Quality
* Data Governance
* Data Lineage
* Data Marts
* Observabilidade

---

## Resultados

O projeto demonstra uma implementação completa de uma plataforma moderna de dados utilizando Google Cloud Platform.

Entre os principais entregáveis estão:

* Arquitetura Lakehouse
* Pipeline End-to-End
* Camadas RAW, BRONZE, SILVER e GOLD
* Modelagem Dimensional
* Framework de Qualidade
* Governança de Dados
* Data Lineage
* Data Marts Analíticos
* Observabilidade Operacional

---

## Autor

**Eli Rodrigues**

Senior Data Engineer

Tecnologias:

GCP • BigQuery • Dataform • SQL • Python • Lakehouse • Data Quality • Data Governance
