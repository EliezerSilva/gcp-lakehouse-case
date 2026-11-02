"""
Trilha local de auditoria do agente de quarentena.

Eventos encadeados por SHA-256.
Sem dados financeiros individuais.
Nao aplica correcoes.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def canonical(data):
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def append_event(path, event):
    """Grava evento com lock exclusivo e verificacao da cadeia."""
    import fcntl
    import os

    path = Path(path)

    if not isinstance(event, dict):
        raise TypeError("Evento invalido.")

    required = {"event_id", "record_hash", "action", "status"}
    allowed = required | {"evidence_id", "attempt"}

    if not required.issubset(event):
        raise ValueError("Campos obrigatorios ausentes.")

    if set(event) - allowed:
        raise ValueError("Campos nao autorizados.")

    if not all(
        isinstance(event[k], str) and event[k].strip()
        for k in required
    ):
        raise ValueError("Campos obrigatorios invalidos.")

    path.parent.mkdir(parents=True, exist_ok=True)

    lock_path = path.with_name(path.name + ".lock")

    fd = os.open(
        lock_path,
        os.O_CREAT | os.O_RDWR,
        0o600,
    )

    try:
        with os.fdopen(fd, "r+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            entries = []
            if path.exists():
                entries = [
                    json.loads(line)
                    for line in path.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    if line.strip()
                ]

            previous = "GENESIS"
            existing_ids = {}

            for entry in entries:
                body = {
                    k: v for k, v in entry.items()
                    if k != "entry_hash"
                }

                digest = hashlib.sha256(
                    canonical(body).encode("utf-8")
                ).hexdigest()

                if (
                    entry.get("previous_hash") != previous
                    or entry.get("entry_hash") != digest
                ):
                    raise RuntimeError(
                        "Historico de auditoria inconsistente."
                    )

                event_id = entry["event_id"]

                if event_id in existing_ids:
                    raise RuntimeError(
                        "Evento duplicado no historico."
                    )

                existing_ids[event_id] = entry
                previous = digest

            if event["event_id"] in existing_ids:
                existing = existing_ids[event["event_id"]]

                stored_event = {
                    k: v for k, v in existing.items()
                    if k not in {
                        "timestamp_utc",
                        "previous_hash",
                        "entry_hash",
                    }
                }

                if stored_event != event:
                    raise RuntimeError(
                        "event_id reutilizado com conteudo diferente."
                    )

                return False

            entry = {
                **event,
                "timestamp_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
                "previous_hash": previous,
            }

            entry["entry_hash"] = hashlib.sha256(
                canonical(entry).encode("utf-8")
            ).hexdigest()

            with path.open("a", encoding="utf-8") as output:
                output.write(canonical(entry) + "\n")
                output.flush()
                os.fsync(output.fileno())

            os.chmod(path, 0o600)
            return True

    finally:
        # fdopen fecha o descritor inclusive em caso de erro.
        pass
