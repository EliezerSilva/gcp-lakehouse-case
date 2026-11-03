"""
Controlador de investigacao recursiva da quarentena.

Nao modifica dados.
Nao autoriza correcoes.
Nao realiza chamadas externas.
"""

import json
from pathlib import Path

MAX_ATTEMPTS = 3

INPUT = Path("/tmp/gcp_quarantine_agent_diagnosis.jsonl")


def investigate(diagnosis, evidence=None):
    """
    Processa ate MAX_ATTEMPTS evidencias distintas em sequencia.

    Apenas evidencias verificadas sao consideradas.
    Nenhuma evidencia autoriza correcao automaticamente.
    """
    evidence = evidence or []

    if not isinstance(evidence, list):
        raise ValueError("Evidencias devem ser uma lista.")

    history = []
    seen_evidence = set()
    record_hash = diagnosis["record_hash"]

    # Preserva a ordem de apresentacao das evidencias.
    candidates = [
        item for item in evidence
        if isinstance(item, dict)
        and item.get("record_hash") == record_hash
        and item.get("verified") is True
        and isinstance(item.get("evidence_id"), str)
        and item["evidence_id"].strip()
    ]

    for item in candidates:
        evidence_id = item["evidence_id"]

        if evidence_id in seen_evidence:
            continue

        if len(history) >= MAX_ATTEMPTS:
            break

        seen_evidence.add(evidence_id)

        history.append({
            "attempt": len(history) + 1,
            "status": "EVIDENCE_RECEIVED",
            "evidence_id": evidence_id,
        })

    if not history:
        history.append({
            "attempt": 1,
            "status": "NO_NEW_EVIDENCE",
        })

    return {
        "record_hash": record_hash,
        "state": "ESCALATED_FOR_HUMAN_REVIEW",
        "approved": False,
        "attempts": len(history),
        "history": history,
    }



def main():
    if not INPUT.is_file():
        raise RuntimeError("Diagnostico anterior nao encontrado.")

    diagnoses = []

    for line in INPUT.read_text(encoding="utf-8").splitlines():
        if line.startswith('{"record_hash":'):
            diagnoses.append(json.loads(line))

    if len(diagnoses) != len({
        d["record_hash"] for d in diagnoses
    }):
        raise RuntimeError("Diagnosticos duplicados.")

    from importlib import import_module

    evidence_module = import_module("10_quarantine_evidence")
    evidence_results = evidence_module.investigate_versions()

    evidence_by_hash = {
        item["record_hash"]: item
        for item in evidence_results
    }

    if len(evidence_by_hash) != len(evidence_results):
        raise RuntimeError("Evidencias duplicadas.")

    diagnosis_hashes = {d["record_hash"] for d in diagnoses}

    if set(evidence_by_hash) != diagnosis_hashes:
        raise RuntimeError(
            "Quarentena e diagnosticos nao correspondem."
        )

    results = []

    for diagnosis in diagnoses:
        evidence = evidence_by_hash[diagnosis["record_hash"]]

        if evidence["state"] == "RESOLVED_BY_APPROVED_VERSION":
            results.append({
                "record_hash": diagnosis["record_hash"],
                "state": "RESOLVED_BY_APPROVED_VERSION",
                "approved": False,
                "attempts": 0,
                "history": [{
                    "attempt": 0,
                    "status": "APPROVED_VERSION_FOUND",
                }],
            })
        else:
            results.append(investigate(diagnosis))

    print("=== QUARANTINE RECOVERY LOOP ===")
    print("Registros:", len(results))
    print("Representados na Silver:", sum(
        r["state"] == "RESOLVED_BY_APPROVED_VERSION"
        for r in results
    ))
    print("Escalados:", sum(
        r["state"] == "ESCALATED_FOR_HUMAN_REVIEW"
        for r in results
    ))
    print("Aprovados:", sum(r["approved"] for r in results))
    print("Tentativas maximas:", MAX_ATTEMPTS)
    print("Nenhuma correcao aplicada.")


if __name__ == "__main__":
    main()


def investigate_with_tool(diagnosis, evidence_tool):
    """
    Executa buscas sequenciais, com limite de tentativas.

    evidence_tool e uma funcao injetada para investigacao.
    O controlador nao confia automaticamente na ferramenta
    nem aprova alteracoes.
    """
    if not callable(evidence_tool):
        raise TypeError("Ferramenta de evidencia invalida.")

    record_hash = diagnosis["record_hash"]
    seen = set()
    history = []

    for attempt in range(1, MAX_ATTEMPTS + 1):
        result = evidence_tool(
            record_hash=record_hash,
            attempt=attempt,
            previous_evidence_ids=tuple(sorted(seen)),
        )

        if result is None:
            history.append({
                "attempt": attempt,
                "status": "NO_NEW_EVIDENCE",
            })
            break

        if not isinstance(result, dict):
            raise RuntimeError("Resposta de ferramenta invalida.")

        evidence_id = result.get("evidence_id")
        evidence_record = result.get("record_hash")

        if (
            not isinstance(evidence_id, str)
            or not evidence_id.strip()
            or evidence_record != record_hash
        ):
            history.append({
                "attempt": attempt,
                "status": "INVALID_EVIDENCE",
            })
            break

        if evidence_id in seen:
            history.append({
                "attempt": attempt,
                "status": "REPEATED_EVIDENCE",
            })
            break

        seen.add(evidence_id)

        history.append({
            "attempt": attempt,
            "status": "EVIDENCE_FOUND_UNVERIFIED",
            "evidence_id": evidence_id,
        })

    return {
        "record_hash": record_hash,
        "state": "ESCALATED_FOR_HUMAN_REVIEW",
        "approved": False,
        "attempts": len(history),
        "history": history,
    }
