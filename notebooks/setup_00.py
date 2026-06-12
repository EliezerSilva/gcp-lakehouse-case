# Alias de importação — permite `from setup_00 import ...` nos notebooks
# O arquivo principal é 00_setup.py; este módulo apenas re-exporta tudo.

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Renomeia o módulo principal para ser importável sem prefixo numérico
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "setup_00",
    os.path.join(os.path.dirname(__file__), "00_setup.py")
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

# Re-exporta símbolos públicos explicitamente
PROJECT_ID       = _mod.PROJECT_ID
REGION           = _mod.REGION
BUCKET           = _mod.BUCKET
SOURCE_PATH      = _mod.SOURCE_PATH
DATASETS         = _mod.DATASETS
SOURCE_MAP       = _mod.SOURCE_MAP
logger           = _mod.logger
get_bq_client    = _mod.get_bq_client
get_gcs_client   = _mod.get_gcs_client
new_execution_id = _mod.new_execution_id
now_utc          = _mod.now_utc
ensure_ingestion_log = _mod.ensure_ingestion_log
log_execution    = _mod.log_execution
run_query        = _mod.run_query
run_query_to_df  = _mod.run_query_to_df
load_df_to_bq    = _mod.load_df_to_bq
validate_environment = _mod.validate_environment
StepLogger       = _mod.StepLogger
