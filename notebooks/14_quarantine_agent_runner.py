"""
Executor de verificacao do AI Quarantine Recovery Agent.

Modo READ_ONLY.
Nao consulta nem modifica o BigQuery.
Nao executa correcoes.
"""

import hashlib
import json
from collections import Counter
from pathlib import Path

EXECUTION_ID = "11f2d8015653111d"

RUNTIME = (
    Path(__file__).resolve().parent.parent
    / "runtime"
    / "quarantine"
    / EXECUTION_ID
)



def verify_quarantine_snapshot(plan):
    """Compara identidades e regras com o BigQuery atual."""
    from google.cloud import bigquery

    project = "eli-lakehouse-gcp"
    client = bigquery.Client(project=project)

    if client.project != project:
        raise RuntimeError("Projeto GCP incorreto.")

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=209715200,
        use_query_cache=False,
    )

    sql = f"""
    SELECT record_hash, quality_errors
    FROM `{project}.quality.quarantine_records`
    """

    rows = list(client.query(
        sql,
        location="us-central1",
        job_config=config,
    ).result())

    local = {
        item["record_hash"]: tuple(sorted(item["quality_errors"]))
        for item in plan
    }

    remote = {
        row.record_hash: tuple(sorted(row.quality_errors))
        for row in rows
    }

    if len(local) != len(plan) or len(remote) != len(rows):
        raise RuntimeError("Identidades duplicadas.")

    if local != remote:
        raise RuntimeError(
            "Quarentena alterada: execucao bloqueada."
        )

    print("PASS: inventario sincronizado com BigQuery.")


def run():
    manifest_path = RUNTIME / "manifest.json"
    plan_path = RUNTIME / "recovery_plan.json"
    events_path = RUNTIME / "events.jsonl"

    for path in (manifest_path, plan_path, events_path):
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(
                f"Artefato ausente ou inseguro: {path.name}"
            )

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    plan_raw = plan_path.read_bytes()
    plan = json.loads(plan_raw)

    if hashlib.sha256(plan_raw).hexdigest() != manifest["plan_sha256"]:
        raise RuntimeError("Plano divergente do manifesto.")

    if manifest["plan_sha256"][:16] != EXECUTION_ID:
        raise RuntimeError("Identidade de execucao divergente.")

    if manifest["corrections_applied"] != 0:
        raise RuntimeError("Manifesto informa correcoes aplicadas.")

    verify_quarantine_snapshot(plan)

    if len(plan) != 19:
        raise RuntimeError("Cardinalidade inesperada.")

    expected = {
        item["record_hash"]: (
            item["classification"],
            item["recommended_action"],
        )
        for item in plan
    }

    if len(expected) != len(plan):
        raise RuntimeError("Identidades duplicadas no plano.")

    # As classificacoes iniciais devem existir exatamente uma vez.
    # Eventos posteriores sao validados separadamente.
    candidates = sorted(
        (
            item for item in plan
            if item["classification"] == "INSUFFICIENT_EVIDENCE"
        ),
        key=lambda item: item["record_hash"],
    )

    if len(candidates) != 5:
        raise RuntimeError("Inventario de candidatos inesperado.")

    investigation_record = candidates[0]["record_hash"]

    expected_investigation_id = hashlib.sha256(
        (
            EXECUTION_ID + ":" + investigation_record
            + ":READ_ONLY_INTEGRATION_TEST"
        ).encode("utf-8")
    ).hexdigest()

    previous = "GENESIS"
    classified = set()
    investigations = set()
    event_ids = set()
    event_count = 0

    with events_path.open(encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                raise RuntimeError("Linha vazia no historico.")

            entry = json.loads(line)
            event_count += 1

            body = {
                key: value
                for key, value in entry.items()
                if key != "entry_hash"
            }

            canonical = json.dumps(
                body,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            )

            digest = hashlib.sha256(
                canonical.encode("utf-8")
            ).hexdigest()

            if (
                entry.get("previous_hash") != previous
                or entry.get("entry_hash") != digest
            ):
                raise RuntimeError("Cadeia de auditoria invalida.")

            record_hash = entry["record_hash"]
            event_id = entry["event_id"]

            if event_id in event_ids:
                raise RuntimeError("ID de evento duplicado.")
            event_ids.add(event_id)

            if record_hash not in expected:
                raise RuntimeError("Registro desconhecido no historico.")

            if entry["action"] == "READ_ONLY_INTEGRATION_TEST":
                if (
                    record_hash != investigation_record
                    or event_id != expected_investigation_id
                    or entry["status"] != "ESCALATED_FOR_HUMAN_REVIEW"
                    or event_id in investigations
                ):
                    raise RuntimeError("Investigacao nao reconhecida.")

                investigations.add(event_id)

            else:
                if record_hash in classified:
                    raise RuntimeError("Classificacao duplicada.")

                if (
                    entry["status"],
                    entry["action"],
                ) != expected[record_hash]:
                    raise RuntimeError("Classificacao divergente.")

                expected_id = hashlib.sha256(
                    (
                        EXECUTION_ID
                        + ":"
                        + record_hash
                        + ":CLASSIFIED"
                    ).encode("utf-8")
                ).hexdigest()

                if event_id != expected_id:
                    raise RuntimeError("ID de classificacao invalido.")

                classified.add(record_hash)

            previous = digest

    if classified != set(expected):
        raise RuntimeError("Classificacoes incompletas.")

    if len(investigations) != 1 or event_count != 20:
        raise RuntimeError("Quantidade de eventos inesperada.")

    summary = Counter(
        item["classification"]
        for item in plan
    )

    print("=== AI QUARANTINE AGENT — AUDITORIA ===")
    print("Execution ID:", EXECUTION_ID)
    print("Registros:", len(plan))
    print("Eventos:", event_count)
    print("Classificacoes auditadas:", len(classified))
    print("Investigacoes auditadas:", len(investigations))
    print("Classificacoes:", dict(sorted(summary.items())))
    print("Hash final:", previous)
    print("Correcoes aplicadas:", 0)
    print("PASS: execucao persistida consistente.")
    print("Modo: READ_ONLY.")


if __name__ == "__main__":
    run()
