"""Teste de integração: recomendação, Bronze e controlador."""

import importlib
import json
import subprocess
import sys
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS = ROOT / "notebooks"
EXECUTION_ID = "11f2d8015653111d"
PROJECT = "eli-lakehouse-gcp"

sys.path.insert(0, str(NOTEBOOKS))


def main():
    subprocess.run(
        [
            sys.executable,
            str(NOTEBOOKS / "14_quarantine_agent_runner.py"),
        ],
        check=True,
    )

    plan = json.loads(
        (
            ROOT / "runtime/quarantine"
            / EXECUTION_ID / "recovery_plan.json"
        ).read_text(encoding="utf-8")
    )

    candidates = sorted(
        (
            item for item in plan
            if item["classification"] == "INSUFFICIENT_EVIDENCE"
        ),
        key=lambda item: item["record_hash"],
    )

    if len(candidates) != 5:
        raise RuntimeError("Inventario inesperado.")

    selected = candidates[0]

    if selected["quality_errors"] != ["DQ004_DISCOUNT_INVALID"]:
        raise RuntimeError("Caso diferente do teste anterior.")

    # Recomendacao observada na chamada Gemini anterior.
    # Nao representa uma nova inferencia nem evidencia validada.
    recommendation = {
        "action": "CHECK_SOURCE_AMOUNTS",
        "requires_human_approval": True,
        "source": "PREVIOUS_GEMINI_TEST",
        "verified": False,
    }

    if recommendation["action"] != "CHECK_SOURCE_AMOUNTS":
        raise RuntimeError("Acao nao suportada.")

    bronze = importlib.import_module("12_bronze_evidence_tool")
    loop = importlib.import_module("09_quarantine_recovery_loop")

    client = bigquery.Client(project=PROJECT)

    if client.project != PROJECT:
        raise RuntimeError("Projeto incorreto.")

    evidence = bronze.fetch_evidence(
        selected["record_hash"],
        client,
    )

    if not isinstance(evidence, list) or len(evidence) > 20:
        raise RuntimeError("Conjunto de evidencias inesperado.")

    if any(
        not isinstance(item, dict)
        or not isinstance(item.get("evidence_id"), str)
        or not item["evidence_id"]
        for item in evidence
    ):
        raise RuntimeError("Identidade de evidencia invalida.")

    calls = 0

    def evidence_tool(*, record_hash, attempt, previous_evidence_ids):
        nonlocal calls
        calls += 1

        if record_hash != selected["record_hash"]:
            raise RuntimeError("Registro inesperado.")

        if attempt not in (1, 2, 3):
            raise RuntimeError("Limite de tentativas excedido.")

        for item in evidence:
            if item["evidence_id"] not in previous_evidence_ids:
                return item

        return None

    result = loop.investigate_with_tool(
        {
            "record_hash": selected["record_hash"],
            "quality_errors": selected["quality_errors"],
        },
        evidence_tool,
    )

    if result.get("approved") is not False:
        raise RuntimeError("Aprovacao indevida.")

    if result.get("state") != "ESCALATED_FOR_HUMAN_REVIEW":
        raise RuntimeError("Estado final inesperado.")

    if calls > 3:
        raise RuntimeError("Limite de chamadas excedido.")

    if any(
        item.get("status") == "EVIDENCE_APPROVED"
        for item in result.get("history", [])
    ):
        raise RuntimeError("Evidencia aprovada indevidamente.")

    summary = {
        "mode": "READ_ONLY_INTEGRATION_TEST",
        "gemini_called": False,
        "recommendation": recommendation["action"],
        "recommendation_verified": False,
        "quality_errors": selected["quality_errors"],
        "bronze_evidence_count": len(evidence),
        "controller_attempts": result["attempts"],
        "final_state": result["state"],
        "approved": False,
        "apply_correction": False,
        "audit_event_written": False,
    }

    print("=== RESULTADO INTEGRADO ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("PASS: integracao deterministica validada.")


if __name__ == "__main__":
    main()
