#!/usr/bin/env python3
# =============================================================================
# run_pipeline.py
# Orquestrador local — executa a pipeline completa em sequência
#
# Uso: python run_pipeline.py [--layer raw|bronze|silver|gold|quality]
# Sem argumento: executa todas as camadas
# =============================================================================

import sys
import time
import logging
import argparse
import importlib.util
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("pipeline")

NOTEBOOKS_DIR = os.path.dirname(__file__)

STEPS = [
    ("setup",   "00_setup.py",           "validate_environment"),
    ("raw",     "01_raw_ingestion.py",   "run_raw_ingestion"),
    ("bronze",  "02_bronze_transform.py","run_bronze_transform"),
    ("quality_gate", "06_quality_gate.py", "run_quality_gate"),
    ("silver",  "03_silver_integrate.py","run_silver_integrate"),
    ("gold",    "04_gold_model.py",      "run_gold_model"),
    ("quality", "05_quality_checks.py",  "run_quality_checks"),
]


def load_and_run(filename: str, function_name: str):
    filepath = os.path.join(NOTEBOOKS_DIR, filename)
    spec = importlib.util.spec_from_file_location("_module", filepath)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if function_name == "validate_environment":
        from setup_00 import get_bq_client, validate_environment, ensure_ingestion_log
        bq = get_bq_client()
        validate_environment(bq)
        ensure_ingestion_log(bq)
    else:
        fn = getattr(mod, function_name)
        fn()


def main():
    parser = argparse.ArgumentParser(description="Lakehouse GCP Pipeline")
    parser.add_argument("--layer", choices=["setup","raw","bronze","quality_gate","silver","gold","quality"],
                        default=None, help="Executa apenas a camada especificada")
    args = parser.parse_args()

    if args.layer in ("silver", "gold"):
        raise SystemExit(
            "Execucao isolada bloqueada: utilize o fluxo completo "
            "apos validar o Quality Gate."
        )

    steps_to_run = STEPS if args.layer is None else [
        s for s in STEPS if s[0] == args.layer
    ]

    if not steps_to_run:
        logger.error(f"Layer não encontrada: {args.layer}")
        sys.exit(1)

    total_start = time.time()
    failures = []

    for layer_name, filename, fn_name in steps_to_run:
        logger.info(f"{'='*60}")
        logger.info(f"Iniciando: {layer_name.upper()} ({filename})")
        logger.info(f"{'='*60}")
        step_start = time.time()
        try:
            load_and_run(filename, fn_name)
            elapsed = round(time.time() - step_start, 1)
            logger.info(f"[OK] {layer_name.upper()} concluído em {elapsed}s")
        except Exception as e:
            elapsed = round(time.time() - step_start, 1)
            logger.error(f"[ERRO] {layer_name.upper()} falhou após {elapsed}s: {e}")
            failures.append((layer_name, str(e)))
            # Interrompe pipeline em falha de camada crítica (não quality)
            if layer_name in ("setup", "raw", "bronze", "quality_gate", "silver", "gold"):
                logger.error("Camada crítica com falha — pipeline interrompida.")
                sys.exit(1)

    total_elapsed = round(time.time() - total_start, 1)
    logger.info(f"{'='*60}")
    if failures:
        logger.warning(f"Pipeline concluída com {len(failures)} falha(s) em {total_elapsed}s")
        for layer_name, err in failures:
            logger.warning(f"  {layer_name}: {err}")
        sys.exit(1)
    else:
        logger.info(f"Pipeline concluída com sucesso em {total_elapsed}s")


if __name__ == "__main__":
    main()
