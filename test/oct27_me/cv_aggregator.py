# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching Engine - Client Value Aggregator (Unity Catalog)
# MAGIC Builds FY / FY-quarter / rolling-12-month client value aggregates into the managed table `raw.client_codes_aggregates`, then syncs that MERGE's change data feed (inserts, deletes,updates) to the Matching Engine server. The table is restored if the server sync fails.
# MAGIC

# COMMAND ----------

import os
import json
from typing import List
from datetime import date, timedelta, datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable
from httpx import (
    Client,
    Limits,
    Timeout
)

# COMMAND ----------

class CustomFormatter(logging.Formatter):
    def format(self, record):
        now = datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f":{now.microsecond // 1000:03d}"
        method_name = record.funcName
        log_level = record.levelname
        message = record.getMessage()

        return f"{timestamp} {log_level} --- [{method_name}]: {message}"

# COMMAND ----------

def get_logger(name=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

# COMMAND ----------

logger = get_logger(__name__)

# COMMAND ----------

def partition(records, size: int = 1_000):
    for index in range(0, len(records), size):
        yield records[index:index + size]

# COMMAND ----------

# DBTITLE 1,Configuration and catalog context
dbutils.widgets.text('CATALOG', '')
dbutils.widgets.text('cip_client_id', '')
dbutils.widgets.text('cip_client_secret', '')
dbutils.widgets.text('auth_and_date_formats', '')
dbutils.widgets.text('auth_server', '')
# Retained for ADF parameter compatibility only. Under UC the aggregate table is resolved by
# name, so 'client_matches_aggregate_location' is no longer used.
dbutils.widgets.text('true_match_locations', '')

CATALOG_NAME = dbutils.widgets.get('CATALOG').strip()

if not CATALOG_NAME:
    raise ValueError('CATALOG widget must be set (e.g. d4001-centralus-tdsci-cipcatalog).')

# Set before the %run utilities below so any table they resolve uses this catalog.
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

CLIENT_CODES_AGGREGATES_TABLE = 'raw.client_codes_aggregates'

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_match_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_udfs.py"

# COMMAND ----------

# DBTITLE 1,Delta version helpers (table-name based)
def _latest_version(table_name: str) -> int:
    return DeltaTable.forName(spark, table_name).history(1).select('version').first()['version']


def _latest_merge_version_after(table_name: str, after_version: int):
    """Version of the newest MERGE committed after `after_version`, or None if the MERGE made no commit.

    The latest table version cannot be used directly: auto-compaction and UC predictive optimization
    can add OPTIMIZE commits after the MERGE, and their change feed is empty.
    """
    row = (
        DeltaTable.forName(spark, table_name).history(20)
        .filter((F.upper(F.col('operation')) == 'MERGE') & (F.col('version') > after_version))
        .select('version')
        .orderBy(F.col('version').desc())
        .first()
    )
    return row['version'] if row else None

# COMMAND ----------

# DBTITLE 1,Server sync
def to_insert_records(records: List[Row]):
    return [{
                'id': r['id'].upper(),
                'clientMappingSource': r['client_mapping_source'],
                'clientCode': r['client_code'],
                'originatingSource': r['originating_source'],
                'timeFrame': r['time_frame'],
                'timeFrameValue': r['time_frame_value'],
                'value': float(r['value']) if r['value'] else 0.0
                } for r in records
            ]


def to_patch_records(records: List[Row]):
    return [{'id': r['id'].upper(), 'value': float(r['value']) if r['value'] else 0.0} for r in records]


def _submit_batches(httpx_client: Client, url: str, records: list, method: str) -> None:
    with ThreadPoolExecutor(max_workers=5) as executor:
        jobs = [
            executor.submit(_perform_operation, httpx_client, url, payload_list, method)
            for payload_list in partition(records, size=2_000)
        ]

        for fut in as_completed(jobs):
            response = fut.result()

            # FIX: previously only 429/5xx raised; 4xx responses were silently ignored,
            # leaving the server out of sync with the table.
            if response is not None:
                response.raise_for_status()


def process_inserts(httpx_client: Client, cdf: DataFrame):
    logger.info('Getting inserts if any')

    insert_df = (
        cdf.filter("_change_type = 'insert'")
            .select('id', 'client_mapping_source', 'client_code', 'originating_source',
                    'time_frame', 'time_frame_value', 'value')
    )
    records = to_insert_records(insert_df.collect())

    if records:
        logger.info(f'Saving this many records to the server: {len(records)}')
        _submit_batches(
            httpx_client,
            f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-aggregates-bulk/bulk-upload',
            records,
            'POST',
        )


def process_deletes(httpx_client: Client, cdf: DataFrame):
    logger.info('Getting any deletes if any')

    records = [r['id'].upper() for r in cdf.filter("_change_type = 'delete'").select('id').collect()]

    if records:
        logger.info(f'Deleting this many records on the server: {len(records)}')
        _submit_batches(
            httpx_client,
            f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-aggregates-bulk/bulk-delete',
            records,
            'DELETE',
        )


def process_updates(httpx_client: Client, cdf: DataFrame):
    logger.info('Getting updates if any')

    records = to_patch_records(
        cdf.filter("_change_type = 'update_postimage'").select('id', 'value').collect()
    )

    if records:
        logger.info(f'Updating this many records on the server: {len(records)}')
        _submit_batches(
            httpx_client,
            f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-aggregates-bulk/bulk-patch',
            records,
            'PATCH',
        )

# COMMAND ----------

# DBTITLE 1,Create managed aggregate table
logger.info('Starting Revenue update flow')

spark.conf.set("spark.sql.ansi.enabled", "false")

logger.info('Creating table if necessary')

with capture_errors('matching_engine_cv_aggregate_table'):
    # Managed table (no LOCATION) per the UC managed-table-only policy.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_CODES_AGGREGATES_TABLE} (
            id STRING NOT NULL,
            client_mapping_source STRING NOT NULL,
            client_code STRING,
            originating_source STRING,
            time_frame STRING NOT NULL,
            time_frame_value INT NOT NULL,
            value DECIMAL(18, 2)
        ) USING DELTA
        TBLPROPERTIES (
            delta.enableChangeDataFeed = true,
            delta.autoOptimize.autoCompact = true,
            delta.autoOptimize.optimizeWrite = true
        )
    """)

    # The server sync depends on CDF. Enforce it even when the table was pre-created by the
    # migration script (CREATE ... IF NOT EXISTS would not apply the properties above).
    spark.sql(f"""
        ALTER TABLE {CLIENT_CODES_AGGREGATES_TABLE}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

# COMMAND ----------

# DBTITLE 1,Build aggregates
logger.info('Creating window functions for each of the revenue types...')

window = (
    Window.partitionBy('source_system', 'counterparty_code')
        .orderBy('trade_date')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
window_fiscal_year = (
    Window.partitionBy('source_system', 'counterparty_code', 'fiscal_year')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
window_fiscal_year_quarter = (
    Window.partitionBy('source_system', 'counterparty_code', 'fiscal_year', 'fiscal_year_quarter')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

logger.info('Getting clean version of all dates')

df = clean_all_dates(spark_session=spark, with_trailing_sum=False)

logger.info('Creating fiscal time frame transformations...')

df_fiscal_year = (
    df.withColumn('fiscal_year',
                  F.when(
                      F.month(F.col('trade_date')) < 11,
                      F.year(F.col('trade_date'))
                      ).otherwise(
                          F.year(F.col('trade_date')) + 1
                          )
                  ).withColumn('fiscal_year_sum', F.sum('client_value_usd').over(window_fiscal_year))
        .withColumn('fiscal_year_total_trades', F.count('*').over(window_fiscal_year))
        .withColumn('fiscal_year_quarter',
                    F.when(F.month(F.col('trade_date')).isin(11, 12, 1), F.lit(1))
                     .when(F.month(F.col('trade_date')).isin(2, 3, 4), F.lit(2))
                     .when(F.month(F.col('trade_date')).isin(5, 6, 7), F.lit(3))
                     .otherwise(F.lit(4))
        )
        .withColumn('fiscal_year_quarter_total_trades', F.count('*').over(window_fiscal_year_quarter))
        .withColumn('fiscal_year_quarter_sum', F.sum('client_value_usd').over(window_fiscal_year_quarter))
)

today = date.today()
last_twelve = today - timedelta(weeks=52)

logger.info('Creating rolling twelve transformations...')

df_trailing_twelve = (
        df_fiscal_year
        .filter((F.col('trade_date') >= F.lit(last_twelve)) & (F.col('trade_date') <= F.lit(today)))
        .withColumn('rolling_12_month_sum', F.sum('client_value_usd').over(window))
        )

columns = [
    'source_system',
    'counterparty_code',
    'raw_client_name',
    'fiscal_year',
    'fiscal_year_quarter',
    'fiscal_year_total_trades',
    'fiscal_year_quarter_total_trades',
    'fiscal_year_sum',
    'fiscal_year_quarter_sum',
    'rolling_12_month_sum'
]

df_fiscal_year = df_fiscal_year.select(columns[0:-1]).distinct()
df_trailing_twelve = df_trailing_twelve.select(*columns).distinct()

logger.info('Joining fiscal and rolling twelve sums...')

df_all_sums = (
    df_fiscal_year.alias('fiscal_df')
        .join(df_trailing_twelve.alias('trailing_df').select('source_system', 'counterparty_code', 'rolling_12_month_sum'),
              on=['source_system', 'counterparty_code'], how='leftouter')
        .withColumn('rolling_12_month_sum', F.coalesce(F.col('rolling_12_month_sum'), F.lit(0.0)))
        .distinct()
)

logger.info('Creating normalized transformations...')

df_fiscal_years = (
    df_all_sums
        .withColumn('client_mapping_source', F.lit('C360'))
        .withColumn('client_code', F.col('counterparty_code'))
        .withColumn('originating_source', F.col('source_system'))
        .withColumn('time_frame', F.concat(F.lit('FY'), F.col('fiscal_year')))
        .withColumn('time_frame_value', F.lit(12))
        .withColumn('value', F.col('fiscal_year_sum').cast(DecimalType(18, 2)))
        .drop(*columns)
        .distinct()
)

df_fiscal_years_quarters = (
    df_all_sums
        .withColumn('client_mapping_source', F.lit('C360'))
        .withColumn('client_code', F.col('counterparty_code'))
        .withColumn('originating_source', F.col('source_system'))
        .withColumn('time_frame', F.concat(F.lit('FY'),
                                          F.col('fiscal_year'),
                                          F.lit('Q'),
                                          F.col('fiscal_year_quarter')))
        .withColumn('time_frame_value', F.col('fiscal_year_quarter'))
        .withColumn('value', F.col('fiscal_year_quarter_sum').cast(DecimalType(18, 2)))
        .drop(*columns)
        .distinct()
)

df_trailing_twelve_months = (
    df_all_sums
        .withColumn('client_mapping_source', F.lit('C360'))
        .withColumn('client_code', F.col('counterparty_code'))
        .withColumn('originating_source', F.col('source_system'))
        .withColumn('time_frame', F.lit('ROLLING_TWELVE_MONTHS'))
        .withColumn('time_frame_value', F.lit(12))
        .withColumn('value', F.col('rolling_12_month_sum').cast(DecimalType(18, 2)))
        .drop(*columns)
        .distinct()
)

logger.info('Unioning the transformations...')

df_union = (
    df_fiscal_years
        .unionByName(df_fiscal_years_quarters)
        .unionByName(df_trailing_twelve_months)
        .withColumn('id', generate_uuid7_str())
)

# COMMAND ----------

# DBTITLE 1,Merge into aggregate table
logger.info('Caching union results with id...')

df_union.cache()
df_union.count()  # materialise the cache without pulling every row to the driver

pre_merge_version = _latest_version(CLIENT_CODES_AGGREGATES_TABLE)
client_code_agg_dt = DeltaTable.forName(spark, CLIENT_CODES_AGGREGATES_TABLE)

try:
    with capture_errors('matching_engine_cv_aggregate_merge'):
        # FIX: null-safe (<=>) on the nullable key columns. With '=', a NULL client_code or
        # originating_source never matched, so those rows were deleted and re-inserted with a
        # new id on every run.
        condition = (
            "orig.client_mapping_source = sums.client_mapping_source and "
            "orig.client_code <=> sums.client_code and "
            "orig.originating_source <=> sums.originating_source and "
            "orig.time_frame = sums.time_frame"
        )
        client_code_agg_dt.alias('orig')\
            .merge(df_union.alias('sums').filter(F.col('time_frame').isNotNull()), condition)\
            .whenNotMatchedInsertAll()\
            .whenMatchedUpdate(condition="orig.value <> sums.value", set={
                'client_mapping_source': 'sums.client_mapping_source',
                'client_code': 'sums.client_code',
                'originating_source': 'sums.originating_source',
                'time_frame': 'sums.time_frame',
                'time_frame_value': 'sums.time_frame_value',
                'value': 'sums.value',
            }).whenNotMatchedBySourceDelete().execute()
finally:
    if df_union.is_cached:
        df_union.unpersist()

merge_version = _latest_merge_version_after(CLIENT_CODES_AGGREGATES_TABLE, pre_merge_version)


# COMMAND ----------

# DBTITLE 1,Sync merge changes to the server (rollback on failure)
if merge_version is None:
    logger.info('MERGE produced no commit; nothing to sync to the server')
else:
    logger.info(f'Syncing changes from MERGE version {merge_version} (pre-merge version {pre_merge_version})')

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

    cdf = (
        spark.read.format('delta')
            .option('readChangeFeed', 'true')
            .option('startingVersion', merge_version)
            .option('endingVersion', merge_version)
            .table(CLIENT_CODES_AGGREGATES_TABLE)
    ).cache()

    try:
        process_inserts(HTTPX_CLIENT, cdf)
        process_deletes(HTTPX_CLIENT, cdf)
        process_updates(HTTPX_CLIENT, cdf)
    except Exception as ex:
        logger.exception(f'Exception encountered, restoring {CLIENT_CODES_AGGREGATES_TABLE} '
                         f'to version {pre_merge_version}: {ex}')

        client_code_agg_dt.restoreToVersion(pre_merge_version)

        raise
    finally:
        cdf.unpersist()
        HTTPX_CLIENT.close()

logger.info('Done')
