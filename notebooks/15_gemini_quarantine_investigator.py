"""
Gemini Quarantine Investigator.

- Entrada tecnica minimizada
- Uma chamada por execucao, apenas com --live
- Nenhuma ferramenta de escrita
- Nenhuma aprovacao automatica
- Saida validada deterministicamente
"""

import argparse
import json
from decimal import Decimal

PROJECT = "eli-lakehouse-gcp"
LOCATION = "global"
MODEL = "gemini-3.5-flash"

ALLOWED_ACTIONS = {
    "INVESTIGATE_SOURCE",
    "REQUEST_HUMAN_REVIEW",
    "COLLECT_MORE_EVIDENCE",
}

SYNTHETIC_CASE = {
    "case_type": "FINANCIAL_RECONCILIATION",
    "quality_errors": ["DQ005"],
    "gross_amount": "100.00",
    "discount_amount": "10.00",
    "reported_net_amount": "95.00",
    "synthetic": True,
}


def validate_case(case):
    if case.get("synthetic") is not True:
        raise ValueError(
            "Esta versao aceita apenas casos sinteticos."
        )

    if set(case) != {
        "case_type",
        "quality_errors",
        "gross_amount",
        "discount_amount",
        "reported_net_amount",
        "synthetic",
    }:
        raise ValueError("Campos de entrada nao autorizados.")

    for field in (
        "gross_amount",
        "discount_amount",
        "reported_net_amount",
    ):
        value = Decimal(case[field])
        if not value.is_finite():
            raise ValueError("Valor financeiro nao finito.")


def validate_recommendation(result):
    if not isinstance(result, dict):
        raise ValueError("Resposta deve ser objeto JSON.")

    required = {
        "expected_net",
        "observed_net",
        "requires_human_approval",
        "recommended_action",
    }

    if set(result) != required:
        raise ValueError("Estrutura da resposta invalida.")

    if Decimal(str(result["expected_net"])) != Decimal("90"):
        raise ValueError("Calculo esperado incorreto.")

    if Decimal(str(result["observed_net"])) != Decimal("95"):
        raise ValueError("Valor observado incorreto.")

    if result["requires_human_approval"] is not True:
        raise ValueError("Aprovacao humana obrigatoria.")

    if result["recommended_action"] not in ALLOWED_ACTIONS:
        raise ValueError("Acao nao autorizada.")

    return {
        "classification": "UNVERIFIED_AI_RECOMMENDATION",
        "recommendation": result,
        "approved": False,
        "apply_correction": False,
    }


def investigate(case, live=False):
    validate_case(case)

    if not live:
        return {
            "mode": "DRY_RUN",
            "model_called": False,
            "case": case,
            "approved": False,
            "apply_correction": False,
        }

    from google import genai
    from google.genai import types

    prompt = (
        "Voce e um investigador de qualidade de dados. "
        "Os dados abaixo sao sinteticos. "
        "Calcule o valor liquido esperado usando bruto menos desconto. "
        "Nao invente evidencias, nao aprove correcoes, "
        "nao solicite escritas. "
        "Retorne somente JSON com as chaves "
        "expected_net, observed_net, requires_human_approval, "
        "recommended_action. "
        "recommended_action deve ser INVESTIGATE_SOURCE.\n\n"
        + json.dumps(case, sort_keys=True)
    )

    client = genai.Client(
        vertexai=True,
        project=PROJECT,
        location=LOCATION,
    )

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                max_output_tokens=300,
                response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(
                    thinking_level="MINIMAL"
                ),
            ),
        )

        if not response.text:
            raise RuntimeError("Resposta vazia do Gemini.")

        result = validate_recommendation(
            json.loads(response.text)
        )

        result["mode"] = "LIVE"
        result["model"] = MODEL
        result["model_called"] = True

        if response.usage_metadata:
            result["total_tokens"] = (
                response.usage_metadata.total_token_count
            )

        return result

    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live",
        action="store_true",
        help="Autoriza uma chamada de inferencia ao Vertex AI.",
    )
    args = parser.parse_args()

    result = investigate(
        SYNTHETIC_CASE,
        live=args.live,
    )

    print(json.dumps(
        result,
        indent=2,
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
