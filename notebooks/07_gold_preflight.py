"""
Pre-flight bloqueante da candidata Gold.

Executa apenas consultas SELECT.
Nao publica nem substitui tabelas.
"""

import ast
from pathlib import Path

from google.cloud import bigquery

from setup_00 import PROJECT_ID, REGION

EXPECTED_PROJECT = "eli-lakehouse-gcp"
MAX_BYTES = 209715200


def extract_sql(name):
    source = Path(__file__).with_name("04_gold_model.py")
    tree = ast.parse(source.read_text(encoding="utf-8"))

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(t, ast.Name) and t.id == name
            for t in node.targets
        ):
            continue

        if not isinstance(node.value, ast.JoinedStr):
            raise RuntimeError(f"SQL inesperado: {name}")

        parts = []
        for item in node.value.values:
            if isinstance(item, ast.Constant):
                parts.append(item.value)
            elif (
                isinstance(item, ast.FormattedValue)
                and isinstance(item.value, ast.Name)
                and item.value.id == "P"
            ):
                parts.append(EXPECTED_PROJECT)
            else:
                raise RuntimeError(
                    f"Interpolacao nao autorizada: {name}"
                )

        return "".join(parts).strip().rstrip(";")

    raise RuntimeError(f"SQL nao encontrado: {name}")


def run_gold_preflight():
    if PROJECT_ID != EXPECTED_PROJECT:
        raise RuntimeError("Projeto GCP incorreto.")

    client = bigquery.Client(project=EXPECTED_PROJECT)

    if client.project != EXPECTED_PROJECT:
        raise RuntimeError("Cliente BigQuery incorreto.")

    fato = extract_sql("SQL_FATO_PEDIDOS")

    query = f"""
    WITH fato AS (
    {fato}
    ),
    pedidos AS (
        SELECT
            order_id,
            MAX(desconto_pedido) AS desconto_cabecalho,
            SUM(desconto_rateado_item) AS desconto_rateado
        FROM fato
        GROUP BY order_id
    )
    SELECT
        (SELECT COUNT(*) FROM fato) AS linhas,
        (SELECT COUNT(DISTINCT sk_fato) FROM fato)
            AS chaves_unicas,
        (SELECT COUNT(DISTINCT order_id) FROM fato)
            AS pedidos_distintos,
        (SELECT COUNT(*) FROM (
            SELECT order_id, item_seq
            FROM `{EXPECTED_PROJECT}.silver.silver_pedidos_itens`
            EXCEPT DISTINCT
            SELECT order_id, item_seq FROM fato
        )) AS chaves_ausentes,
        (SELECT COUNT(*) FROM (
            SELECT order_id, item_seq FROM fato
            EXCEPT DISTINCT
            SELECT order_id, item_seq
            FROM `{EXPECTED_PROJECT}.silver.silver_pedidos_itens`
        )) AS chaves_excedentes,
        (SELECT COUNTIF(receita_liquida_item IS NULL) FROM fato)
            AS receitas_nulas,
        (SELECT COUNTIF(
            qualidade_cliente = 'RESOLVIDO'
            AND qualidade_produto = 'RESOLVIDO'
            AND qualidade_canal = 'RESOLVIDO'
            AND qualidade_regiao = 'RESOLVIDO'
        ) FROM fato) AS itens_resolvidos,
        (SELECT COUNTIF(
            qualidade_cliente != 'RESOLVIDO'
            OR qualidade_produto != 'RESOLVIDO'
            OR qualidade_canal != 'RESOLVIDO'
            OR qualidade_regiao != 'RESOLVIDO'
        ) FROM fato) AS itens_pendentes,
        (SELECT COUNTIF(
            ABS(desconto_rateado - desconto_cabecalho) > 0.01
        ) FROM pedidos) AS rateios_divergentes
    """

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES,
        use_query_cache=False,
    )

    row = next(iter(client.query(
        query,
        location=REGION,
        job_config=config,
    ).result()))

    actual = dict(row.items())

    source_query = f"""
    SELECT
        (SELECT COUNT(*)
         FROM `{EXPECTED_PROJECT}.silver.silver_pedidos_itens`)
            AS linhas,
        (SELECT COUNT(*)
         FROM `{EXPECTED_PROJECT}.silver.silver_pedidos_cabecalho`)
            AS pedidos_distintos
    """

    source = next(iter(client.query(
        source_query,
        location=REGION,
        job_config=config,
    ).result()))

    expected = {
        "linhas": source.linhas,
        "chaves_unicas": source.linhas,
        "pedidos_distintos": source.pedidos_distintos,
        "receitas_nulas": 0,
        "chaves_ausentes": 0,
        "chaves_excedentes": 0,
        "rateios_divergentes": 0,
    }

    if (
        actual["itens_resolvidos"] + actual["itens_pendentes"]
        != actual["linhas"]
    ):
        raise RuntimeError(
            "Pre-flight bloqueado: classificacao dimensional incompleta."
        )

    print(
        "AUDITORIA DIMENSIONAL: "
        f"resolvidos={actual['itens_resolvidos']}, "
        f"pendentes={actual['itens_pendentes']}"
    )

    print("=== GOLD PRE-FLIGHT ===")

    for key, value in expected.items():
        received = actual[key]
        status = "PASS" if received == value else "FAIL"
        print(f"{status}: {key}={received}; esperado={value}")

        if received != value:
            raise RuntimeError(
                f"Pre-flight bloqueado: {key}"
            )

    print("PASS: candidata Gold validada.")
    print("Nenhuma tabela foi publicada.")


if __name__ == "__main__":
    run_gold_preflight()
