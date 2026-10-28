# Databricks notebook source
# MAGIC %md
# MAGIC ## Matching Engine - PRISM Full Sync (Unity Catalog)
# MAGIC Full (non-incremental) pull from PRISM. Inserts accounts missing from `raw.prism_data` and
# MAGIC deletes local accounts no longer in PRISM. Existing rows are not updated here (the main
# MAGIC notebook's incremental merge does that).

# COMMAND ----------

from datetime import datetime
import logging
import os

from httpx import (
    Client,
    Limits,
    Timeout
)
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Configuration and catalog context
dbutils.widgets.text('CATALOG', '')
dbutils.widgets.text('prism_web_server_base_url', '')
dbutils.widgets.text('prism_auth_server', '')
dbutils.widgets.text('prism_client_id', '')
dbutils.widgets.text('prism_client_secret', '')

CATALOG_NAME = dbutils.widgets.get('CATALOG').strip()

if not CATALOG_NAME:
    raise ValueError('CATALOG widget must be set (e.g. d4001-centralus-tdsci-cipcatalog).')

# Set before the %run utilities below so any table they resolve uses this catalog.
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

PRISM_DATA_TABLE = 'raw.prism_data'

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_data_extractors.py"

# COMMAND ----------

# DBTITLE 1,Logging and HTTP client
class CustomFormatter(logging.Formatter):
    def format(self, record):
        now = datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f":{now.microsecond // 1000:03d}"
        method_name = record.funcName
        log_level = record.levelname
        message = record.getMessage()

        return f"{timestamp} {log_level} --- [{method_name}]: {message}"


def get_logger(name=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


logger = get_logger(__name__)

HTTPX_CLIENT = Client(
    http2=False,
    verify=False,
    timeout=Timeout(
        connect=int(os.getenv("MATCHING_ENGINE_CONNECTION_TIMEOUT", "60")),
        read=int(os.getenv("MATCHING_ENGINE_READ_TIMEOUT", "360")),
        write=int(os.getenv("MATCHING_ENGINE_WRITE_TIMEOUT", "360")),
        # FIX: was reading MATCHING_ENGINE_POOL_LIMIT (the connection-count setting)
        pool=int(os.getenv("MATCHING_ENGINE_POOL_TIMEOUT", "60"))
    ),
    limits=Limits(
        max_connections=int(os.getenv("MATCHING_ENGINE_POOL_LIMIT", "40")),
        max_keepalive_connections=int(os.getenv("MATCHING_ENGINE_KEEPALIVE_CONNECTIONS", "20")),
        keepalive_expiry=int(os.getenv("MATCHING_ENGINE_KEEPALIVE_EXPIRY", "60"))
    )
)

# The notebook-provided `spark` session is used as-is. The previous custom SparkSession builder
# (forcing the Hive catalog implementation and spark_catalog overrides) is removed: it targets
# the Hive Metastore and static configs cannot be changed on the running UC session.

# COMMAND ----------

# DBTITLE 1,Fetch full PRISM extract
logger.info('Synchronizing PRISM Delta with PRISM Actual. Any extra records locally will be deleted')

try:
    with capture_errors('matching_engine_prism_full_fetch'):
        logger.info('Fetching PRISM records...')

        # Epoch watermark -> full (non-incremental) PRISM pull.
        df_last_modified = spark.createDataFrame(
            [(datetime.strptime('1970-01-01', '%Y-%m-%d').date(),)], 'last_modified: date'
        )
        prism_records = _get_prism_records(HTTPX_CLIENT, PRISM_WEB_SERVER_BASE_URL, df_last_modified)
finally:
    HTTPX_CLIENT.close()

# FIX: guard against wiping the table. With whenNotMatchedBySourceDelete, an empty extract
# (e.g. PRISM returning total_count 0 during an outage) deleted every row in raw.prism_data.
if not prism_records:
    raise ValueError(
        'PRISM returned no accounts; aborting full sync to avoid deleting all rows in '
        f'{PRISM_DATA_TABLE}.'
    )

logger.info('Converting PRISM records from Spark to Python')

prism_records = [to_prism_rec(r) for r in prism_records]

# COMMAND ----------

# DBTITLE 1,Merge into raw.prism_data (insert new, delete missing)
with capture_errors('matching_engine_prism_full_sync_merge'):
    prism_dt = DeltaTable.forName(spark, PRISM_DATA_TABLE)

    logger.info('Creating Spark DF for records')

    # FIX: use the explicit PRISM_SCHEMA (as the main notebook does). Passing PRISM_FIELDS
    # (names only) made Spark infer types: all-null columns fail inference, and
    # merged_accounts was inferred as STRING against an ARRAY<STRUCT> table column.
    existing_records = spark.createDataFrame(prism_records, PRISM_SCHEMA)

    logger.info('Deleting any extra rows in the Delta Table NOT in PRISM')
    logger.info(f'Current size: {prism_dt.toDF().count()}, Number of records in PRISM: {len(prism_records)}')

    prism_insert_setter = to_prism_sql(PRISM_FIELDS, 'prism_raw', [])

    prism_dt.alias('stored').merge(
        existing_records.alias('prism_raw'),
        'stored.account_id = prism_raw.account_id'
    ).whenNotMatchedInsert(values=prism_insert_setter).whenNotMatchedBySourceDelete().execute()

    logger.info(f'After insert/delete: {prism_dt.toDF().count()}')

logger.info('Done')

# COMMAND ----------

# End of Notebook