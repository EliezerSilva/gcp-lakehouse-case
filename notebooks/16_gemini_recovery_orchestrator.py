"""Orquestrador Gemini: investigação limitada e sem correções."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

PROJECT = "eli-lakehouse-gcp"
MODEL = "gemini-3.5-flash"
EXECUTION_ID = "11f2d8015653111d"

ROOT = Path(__file__).resolve().parent.parent
PLAN = ROOT / "runtime/quarantine" / EXECUTION_ID / "recovery_plan.json"

ALLOWED_ACTIONS = {
    "CHECK_SOURCE_DATE",
    "CHECK_SOURCE_AMOUNTS",
    "CHECK_SOURCE_VERSION",
    "REQUEST_HUMAN_REVIEW",
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    # Falha fechada se a quarentena ou a auditoria divergirem.
    subprocess.run(
        [sys.executable, str(ROOT / "notebooks/14_quarantine_agent_runner.py")],
        check=True,
    )

    records = json.loads(PLAN.read_text(encoding="utf-8"))
    candidates = sorted(
        (
            r for r in records
            if r["classification"] == "INSUFFICIENT_EVIDENCE"
        ),
        key=lambda r: r["record_hash"],
    )

    if len(candidates) != 5:
        raise RuntimeError("Quantidade de candidatos inesperada.")

    selected = candidates[0]

    # Não transmitir record_hash, payload ou valores financeiros.
    context = {
        "classification": "INSUFFICIENT_EVIDENCE",
        "quality_errors": sorted(selected["quality_errors"]),
        "evidence_status": "SOURCE_NOT_CONFIRMED",
    }

    if not args.live:
        print(json.dumps({
            "mode": "DRY_RUN",
            "model_called": False,
            "candidate_count": len(candidates),
            "sanitized_context": context,
            "approved": False,
            "apply_correction": False,
        }, indent=2))
        return

    from google import genai
    from google.genai import types

    client = genai.Client(
        vertexai=True,
        project=PROJECT,
        location="global",
    )

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=(
                "Você investiga erros de qualidade de dados. "
                "Os dados seguintes são códigos técnicos, não "
                "evidências de correção. Não invente informações. "
                "Recomende uma próxima verificação de origem. "
                "Retorne JSON contendo somente: action, reason, "
                "requires_human_approval. "
                "action deve ser um destes valores: "
                + ", ".join(sorted(ALLOWED_ACTIONS))
                + ". requires_human_approval deve ser true. "
                "Nunca aprove nem execute correções.\n"
                + json.dumps(context, sort_keys=True)
            ),
            config=types.GenerateContentConfig(
                temperature=0,
                max_output_tokens=300,
                response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(
                    thinking_level="MINIMAL"
                ),
            ),
        )

        result = json.loads(response.text or "null")

        if not isinstance(result, dict) or set(result) != {
            "action", "reason", "requires_human_approval"
        }:
            raise RuntimeError("Estrutura Gemini inválida.")

        if result["action"] not in ALLOWED_ACTIONS:
            raise RuntimeError("Ação Gemini não autorizada.")

        if (
            result["requires_human_approval"] is not True
            or not isinstance(result["reason"], str)
            or not result["reason"].strip()
            or len(result["reason"]) > 1000
        ):
            raise RuntimeError("Recomendação Gemini inválida.")

        print(json.dumps({
            "mode": "LIVE",
            "classification": "UNVERIFIED_AI_RECOMMENDATION",
            "recommendation": result,
            "approved": False,
            "apply_correction": False,
            "total_tokens": getattr(
                response.usage_metadata,
                "total_token_count",
                None,
            ),
        }, ensure_ascii=False, indent=2))

    finally:
        client.close()


if __name__ == "__main__":
    main()
