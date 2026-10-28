# Databricks notebook source
# MAGIC %md
# MAGIC ## Matching Engine - PRISM Processing State Poller (Unity Catalog)
# MAGIC Publishes unacknowledged rows of `raw.prism_processing_state` to the Matching Engine server and
# MAGIC marks them acknowledged locally. Rows already present on the server (from an earlier run that
# MAGIC failed before acknowledging) are acknowledged without being re-posted.

# COMMAND ----------

# DBTITLE 1,Configuration and catalog context
dbutils.widgets.text('CATALOG', '')
dbutils.widgets.text('client_matching_web_server_base_url', '')
dbutils.widgets.text('prism_processing_state_location', '')     # retained for ADF compatibility; unused under UC
dbutils.widgets.text('client_matched_codes_delta_location', '')  # retained for ADF compatibility; unused under UC
dbutils.widgets.text('cip_client_id', '')
dbutils.widgets.text('cip_client_secret', '')
dbutils.widgets.text('auth_server', '')
dbutils.widgets.text('rivendell_auth_scopes', '')

CATALOG_NAME = dbutils.widgets.get('CATALOG').strip()

if not CATALOG_NAME:
    raise ValueError('CATALOG widget must be set (e.g. d4001-centralus-tdsci-cipcatalog).')

# Set before the %run utilities below so any table they resolve uses this catalog.
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

PRISM_PROCESSING_STATE_TABLE = 'raw.prism_processing_state'

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

# DBTITLE 1,Logging, HTTP client and mappers
from datetime import datetime, timezone
import logging
import os

from httpx import (
    Client,
    Limits,
    Timeout
)
from delta.tables import DeltaTable

SERVER_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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


def _to_second(value: datetime) -> datetime:
    # The server stores processedDatetime to the second; local timestamps may carry fractions.
    return value.replace(microsecond=0)


def to_server_state(record, acknowledge_datetime: datetime):
    message = record['message']
    if message is None or message == 'null':
        message = ''

    return {
        'status': record['status'],
        'statusEnum': record['status_enum'],
        'message': message,
        'runType': record['run_type'],
        'cronSchedule': record['cron_schedule'],
        'processedDatetime': record['processed_datetime'].strftime(SERVER_DATETIME_FORMAT),
        'acknowledgeDatetime': acknowledge_datetime.strftime(SERVER_DATETIME_FORMAT)
    }


def to_local_obj(record):
    return {
        'id': int(record['id']),
        'processed_datetime': datetime.strptime(record['processedDatetime'], SERVER_DATETIME_FORMAT),
        'status': int(record['status']),
        'status_enum': record['statusEnum'],
        'message': record['message'],
        'run_type': record['runType'],
        'cron_schedule': record['cronSchedule'],
        'acknowledge_datetime': datetime.strptime(record['acknowledgeDatetime'], SERVER_DATETIME_FORMAT)
    }


def acknowledge_locally(processed_datetimes: list, acknowledge_datetime: datetime) -> None:
    ack_df = spark.createDataFrame(
        [(ts, acknowledge_datetime) for ts in processed_datetimes],
        'processed_datetime timestamp, acknowledge_datetime timestamp',
    )

    DeltaTable.forName(spark, PRISM_PROCESSING_STATE_TABLE).alias('dt')\
        .merge(ack_df.alias('ack'),
               'dt.processed_datetime = ack.processed_datetime and dt.acknowledge_datetime is null')\
        .whenMatchedUpdate(set={'acknowledge_datetime': 'ack.acknowledge_datetime'})\
        .execute()

# COMMAND ----------

# DBTITLE 1,Publish unacknowledged processing states
logger.info('Checking prism_processing_state for updates...')

try:
    with capture_errors('matching_engine_prism_processing_state_poll'):
        # FIX: snapshot the pending rows once. The previous lazy DataFrame was re-evaluated by the
        # final MERGE, so rows inserted after the POSTs could be acknowledged without being sent,
        # and current_timestamp() differed between the server payload and the Delta table.
        pending_rows = (
            spark.table(PRISM_PROCESSING_STATE_TABLE)
                .filter('acknowledge_datetime is null')
                .orderBy('processed_datetime', ascending=False)
                .collect()
        )

        undated_rows = [r for r in pending_rows if r['processed_datetime'] is None]
        if undated_rows:
            logger.warning(f'Skipping {len(undated_rows)} pending rows with no processed_datetime')

        pending_rows = [r for r in pending_rows if r['processed_datetime'] is not None]

        if not pending_rows:
            logger.info('No new Prism processing times found')
        else:
            logger.info('Saving and sending to server')

            recs = get_server_record(HTTPX_CLIENT,
                                     f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/prism-processing-state?sort=processedDatetime,desc&size=100',
                                     'prismProcessingStates', to_local_obj)

            server_processed_seconds = {
                _to_second(r['processed_datetime']) for r in recs if r['processed_datetime']
            }

            # One acknowledge time for both the server payload and the Delta table.
            acknowledge_datetime = datetime.now(timezone.utc).replace(microsecond=0)

            already_on_server = [
                r for r in pending_rows if _to_second(r['processed_datetime']) in server_processed_seconds
            ]
            to_publish = [
                r for r in pending_rows if _to_second(r['processed_datetime']) not in server_processed_seconds
            ]

            # FIX: rows already on the server were never acknowledged locally and stayed pending.
            acknowledged = [r['processed_datetime'] for r in already_on_server]

            logger.info(f'Already on server (acknowledging only): {len(already_on_server)}')
            logger.info(f'Publishing to server: {len(to_publish)}')

            try:
                for row in to_publish:
                    response = _perform_operation(HTTPX_CLIENT,
                                                  f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/prism-processing-state',
                                                  to_server_state(row, acknowledge_datetime), 'POST')

                    # FIX: POST failures were ignored and the row was still acknowledged locally.
                    if response is not None:
                        response.raise_for_status()

                    acknowledged.append(row['processed_datetime'])
            finally:
                # Acknowledge exactly what reached the server, even if a later POST failed.
                if acknowledged:
                    logger.info(f'Saving to delta table: {len(acknowledged)} acknowledged')

                    acknowledge_locally(acknowledged, acknowledge_datetime)

            logger.info('Done...')
finally:
    HTTPX_CLIENT.close()

# COMMAND ----------

# End of Notebook