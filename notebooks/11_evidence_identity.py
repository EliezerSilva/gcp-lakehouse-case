"""
Identidade deterministica das evidencias da quarentena.

O hash identifica conteudo canonico.
Nao comprova autenticidade da fonte.
"""

import hashlib
import json


def canonical_evidence(source, record_hash, version, values):
    if not isinstance(source, str) or not source.strip():
        raise ValueError("Fonte invalida.")

    if not isinstance(record_hash, str) or not record_hash.strip():
        raise ValueError("Registro invalido.")

    if not isinstance(values, dict):
        raise TypeError("Valores devem ser um dicionario.")

    content = {
        "source": source,
        "record_hash": record_hash,
        "version": version,
        "values": values,
    }

    canonical = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )

    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()

    return {
        "record_hash": record_hash,
        "evidence_id": digest,
        "source": source,
        "version": version,
        "verified": False,
    }
