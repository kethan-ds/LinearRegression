# Databricks notebook source
# MAGIC %md
# MAGIC ## CIP Matching Engine - Main (Unity Catalog)
# MAGIC Syncs client-code matches with the Matching Engine server, pulls PRISM incrementally, refreshes
# MAGIC subordinate data from all_dates, runs the match and publishes new matches and IDF weights.
# MAGIC
# MAGIC All Delta access is by managed table name in the `CATALOG` widget's catalog (`raw.prism_data`,
# MAGIC `raw.subordinate_data`, `raw.client_code_matches`, `raw.prism_last_modified`, `raw.matched_results`).
# MAGIC Change-feed checks read the specific MERGE commit they follow, not the latest table version.

# COMMAND ----------

import os
from typing import (
    NamedTuple,
    List,
    Tuple
)
from collections import (
    Counter,
    OrderedDict
)
from enum import Enum
from datetime import datetime, timedelta
import logging

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.ml.feature import (
    CountVectorizer, NGram, IDF, Tokenizer, StopWordsRemover
)
from delta.tables import DeltaTable
from httpx import (
    Client,
    Limits,
    Timeout
)
from retry import retry
from delta.exceptions import (
    ConcurrentAppendException,
    ConcurrentTransactionException,
    ConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException
)

# COMMAND ----------

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

class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

# COMMAND ----------

dbutils.widgets.text('subordinate_delta_location', '')
dbutils.widgets.text('client_matched_codes_delta_location', '')
dbutils.widgets.text('all_dates_curated_location', '')
dbutils.widgets.text('prism_web_server_base_url', '')
dbutils.widgets.text('prism_auth_server', '')
dbutils.widgets.text('python_datetime_format', '')
dbutils.widgets.text('spark_datetime_format', '')
dbutils.widgets.text('client_matching_web_server_base_url', '')
dbutils.widgets.text('cip_client_id', '')
dbutils.widgets.text('cip_client_secret', '')
dbutils.widgets.text('prism_client_id', '')
dbutils.widgets.text('prism_client_secret', '')
dbutils.widgets.text('auth_server', '')
dbutils.widgets.text('rivendell_auth_scopes', '')
dbutils.widgets.text('trigger_name', '')
dbutils.widgets.text('trigger_start_time', '')
dbutils.widgets.text('trigger_type', '')
dbutils.widgets.text('trigger_schedule', '')
dbutils.widgets.text('CATALOG', '')

# COMMAND ----------

# DBTITLE 1,Catalog context
# Child notebooks do not inherit catalog context. Set it before the %run utilities below so any
# unqualified table they resolve lands in this catalog.
CATALOG_NAME = dbutils.widgets.get('CATALOG').strip()

if not CATALOG_NAME:
    raise ValueError('CATALOG widget must be set (e.g. d4001-centralus-tdsci-cipcatalog).')

spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./access_token_util.py"

# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_udfs.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_data_extractors.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_match_utils.py"

# COMMAND ----------

# DBTITLE 1,Unity Catalog tables (managed) and Delta version helpers
# Resolved by name against the current catalog. Replaces the *_LOCATION ADLS paths, which are not
# accessible under Unity Catalog for managed tables.
PRISM_LAST_MODIFIED_TABLE = 'raw.prism_last_modified'
PRISM_DATA_TABLE = 'raw.prism_data'
SUBORDINATE_DATA_TABLE = 'raw.subordinate_data'
CLIENT_CODE_MATCHES_TABLE = 'raw.client_code_matches'
MATCHED_RESULTS_TABLE = 'raw.matched_results'

CDF_TABLES = [PRISM_DATA_TABLE, SUBORDINATE_DATA_TABLE, CLIENT_CODE_MATCHES_TABLE]


def _latest_version(table_name: str) -> int:
    return DeltaTable.forName(spark, table_name).history(1).select('version').first()['version']


def _merge_version_after(table_name: str, after_version: int):
    """Version of the newest MERGE committed after `after_version`, or None if none was committed.

    The latest table version is not used: auto-compaction and UC predictive optimization add
    OPTIMIZE commits whose change feed is empty, and later MERGEs in the same run (e.g. the
    Prism consolidation) would otherwise be picked up instead of the MERGE being inspected.
    """
    row = (
        DeltaTable.forName(spark, table_name).history(50)
        .filter((F.upper(F.col('operation')) == 'MERGE') & (F.col('version') > after_version))
        .select('version')
        .orderBy(F.col('version').desc())
        .first()
    )
    return row['version'] if row else None


def _change_pivot(cols: List[str], table_name: str, version, filter_cond: str) -> Tuple[DataFrame, DataFrame]:
    """Change feed of one specific commit, pivoted by change type (replaces get_latest_change_pivot)."""
    if version is None:
        changed_df = (
            spark.table(table_name).limit(0)
            .withColumn('_change_type', F.lit(None).cast('string'))
            .withColumn('_commit_version', F.lit(None).cast('long'))
            .withColumn('_commit_timestamp', F.lit(None).cast('timestamp'))
        )
    else:
        changed_df = (
            spark.read.format('delta')
            .option('readChangeFeed', 'true')
            .option('startingVersion', version)
            .option('endingVersion', version)
            .table(table_name)
            .filter(filter_cond)
        )

    columns = [c for c in changed_df.columns if not c.startswith('_')]

    pivot_df = (
        changed_df.groupBy('_commit_version', *cols)
        .pivot('_change_type', ['insert', 'update_preimage', 'update_postimage', 'delete'])
        .agg(F.first(F.struct(*columns)))
    )

    return changed_df, pivot_df

# COMMAND ----------

class PrismData(NamedTuple):
    account_id: str
    account_name: str
    account_status: Status


# COMMAND ----------

@retry(exceptions=(ConcurrentAppendException, ConcurrentTransactionException),
       delay=int(os.getenv(DELTA_MERGE_DELAY, "5")),
       tries=int(os.getenv(DELTA_MERGE_RETRIES, "10")),
       max_delay=int(os.getenv(DELTA_MERGE_MAX_DELAY, "60")),
       backoff=int(os.getenv(DELTA_MERGE_BACKOFF, "2")),
       jitter=(1, 10))
def update_delta_client_matches(client_matches_dt: DeltaTable, df_to_match: DataFrame) -> None:
    cond = 'target.id = sccm.id'

    # merge any matches on the live web server not in DeltaTable
    client_matches_dt.alias('target').merge(df_to_match.alias('sccm'), cond)\
        .whenMatchedUpdate(set={
            'ops_user_id': 'sccm.ops_user_id',
            'account_id': 'sccm.account_id',
            'parent_name': 'sccm.parent_name',
            'account_name': 'sccm.account_name',
            'account_type': 'sccm.account_type',
            'account_subtype': 'sccm.account_subtype',
            'account_status': 'sccm.account_status',
            'client_display_name': 'sccm.client_display_name',
            'client_value_cad': 'sccm.client_value_cad',
            'client_value_usd': 'sccm.client_value_usd',
            'trade_date': 'sccm.trade_date',
            'matching_percentage': 'sccm.matching_percentage',
            'matching_category': 'sccm.matching_category',
            'match_type': 'sccm.match_type',
            'comment': 'sccm.comment',
            'update_time': 'sccm.update_time'
        }).execute()

    logger.info(f'Update complete')

# COMMAND ----------

def handle_matches(spark_session: SparkSession, matches_to_pull: List[str], client_matches_dt: DeltaTable):
    if not matches_to_pull:
        logger.info(f'Nothing to update, all matches are synced')

        return

    bulk_get_url = (
        f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-get'
    )

    client_matches = bulk_retrieve(bulk_get_url, matches_to_pull, HTTPX_CLIENT, to_client_match)

    if not client_matches:
        return
    server_ccm_df = spark_session.createDataFrame(client_matches, CLIENT_CODE_MATCHES_SCHEMA)
    server_ccm_df = server_ccm_df.withColumn('id', F.ucase('id'))

    server_ccm_df = rematch_changes_if_any(server_ccm_df=server_ccm_df)

    logger.info(f'Updating Delta copy with latest matches')

    update_delta_client_matches(client_matches_dt, server_ccm_df)


def update_matches(spark_session: SparkSession, client_matches_dt: DeltaTable):
    logger.info(f'Updating Delta matches (if any found on the server)...')

    notification_url = (
        f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/notifications/search/'
        f'get-mapping-updates-by-user-id?userId={SYSTEM_ID}'
    )

    logger.info(f'Getting any mapping new matches performed by the users...')

    mapping_updates = get_server_record(HTTPX_CLIENT, notification_url, 'notifications', to_notification)

    # get all mapping updates message ids for records to update
    matches_to_pull = list(set(r['message']['id'] for r in mapping_updates))

    logger.info(f'Found {len(matches_to_pull)} server events for updates (user matches)')

    handle_matches(spark_session, matches_to_pull, client_matches_dt)

    notification_ids = [r['id'] for r in mapping_updates]

    logger.info(f'Deleting processed mapping update notifications...')

    delete_server_by_id(HTTPX_CLIENT, notification_ids,
                        f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/notifications-bulk/delete-by-ids')

# COMMAND ----------

def handle_merge_event(spark_session: SparkSession, client_matches_dt:
                       DeltaTable, prism_dt: DeltaTable):
    logger.info(f'Updating Delta broken matches (if any found on the server)...')

    notification_url = (
        f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/notifications/search/'
        f'get-merge-updates-by-user-id?userId={SYSTEM_ID}'
    )

    logger.info(f'Getting any break matches produced by the users merges (if any)...')

    mapping_updates = get_server_record(HTTPX_CLIENT, notification_url, 'notifications', to_notification)

    if not mapping_updates:
        logger.info('No breaking matches found, moving on...')

        return

    # get all mapping updates message ids for records to update
    matches_to_pull = list(set((r['message']['id'].upper(),) for r in mapping_updates))
    accounts_to_remove = list(set((r['message']['accountId'],) for r in mapping_updates))

    logger.info(f'Found {len(matches_to_pull)} server events for updates (user broken matches)')

    broken_df = spark_session.createDataFrame(matches_to_pull, 'id: string')
    removed_df = spark_session.createDataFrame(accounts_to_remove, 'account_id: string')

    logger.info('Deleting broken matches from Delta table...')

    client_matches_dt.alias('target').merge(broken_df.alias('source'),
                                           condition='target.id = source.id').whenMatchedDelete().execute()

    logger.info('Deleting loser account from Prism data (delta table)')

    prism_dt.alias('target').merge(removed_df.alias('source'),
                                  condition='target.account_id = source.account_id').whenMatchedDelete().execute()

    logger.info('Deleting processed merge notifications...')

    delete_server_by_id(HTTPX_CLIENT, [i['id'] for i in mapping_updates],
                       f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/notifications-bulk/delete-by-ids')

# COMMAND ----------

def delete_server_matches(client_matches_dt: DeltaTable):
    logger.info(f'Server records need to be deleted (more matches found on the server than in the DeltaTable), synchronizing...')
    logger.info(f'Getting server ids to compare with...')

    server_ids_df = get_server_ids()

    if server_ids_df.count() == 0:
        logger.info(f'Nothing to delete')

        return

    client_matches_df = client_matches_dt.toDF().withColumn('id', F.ucase('id'))

    logger.info(f'Comparing with DeltaTable to find missing records to delete')

    missing_dfs = (
        server_ids_df.alias('sccm').join(client_matches_df.alias('ct'),
                                       on=['id'], how='leftanti')
    )

    logger.info(f'Performing batch delete (if required) {missing_dfs.count()}...')

    # delete records on the server that do not exist in DeltaTable
    delete_server_by_id(HTTPX_CLIENT, [r['id'] for r in missing_dfs.collect()],
                       f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-delete')

    logger.info(f'Batch delete done')

# COMMAND ----------

def check_for_duplicates(prism_server_data: List[Tuple[str, ...]]) -> None:
    counter = Counter([rec[0] for rec in prism_server_data])
    duplicates = [item for item in counter.items() if item[1] > 1]

    if duplicates:
        error = (
            f'Duplicate account ids found in PRISM, halting '
            f'application:\n{duplicates}'
        )

        raise ValueError(error)

# COMMAND ----------

def adding_processed_server_records(spark_session: SparkSession,
                                   client_matches_dt: DeltaTable,
                                   query_params: str):
    logger.info(f'Adding matches on webserver not in DeltaTable (comparing matches on webserver), synchronizing...')
    logger.info(f'Getting server ids to compare with...')

    if 'POTENTIAL' in query_params:
        logger.info('Also adding potential matches for a full sync')

    server_ids_df = get_server_ids(spark_session, HTTPX_CLIENT, query_params=query_params)

    if server_ids_df.count() == 0:
        logger.info(f'No matches found to compare with')

        return

    client_matches_df = client_matches_dt.toDF().withColumn('id', F.ucase('id'))

    log_msg = (
        'Comparing with DeltaTable to find missing matches on webserver not in '
        'DeltaTable (will pull them from webserver and add to DeltaTable)'
    )

    logger.info(log_msg)

    matches_missing_dfs = (
        server_ids_df.alias('sccm').join(client_matches_df.alias('ct'),
                                       on=['id'], how='leftanti')
    )

    if matches_missing_dfs.isEmpty():
        logger.info('No matches found on server, moving on')

        return

    logger.info(f'Copying matches on webserver not present in DeltaTable found this many: {matches_missing_dfs.count()}...')


    url_for_sync = f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches?{query_params}'

    all_match_records = get_server_record(HTTPX_CLIENT, url_for_sync,
                                         embedded_type='clientCodeMatches',
                                         extractor=to_client_match)

    if not all_match_records:
        logger.info('Found no matches on the server, moving on...')


        return

    server_matches = spark_session.createDataFrame(all_match_records,
                                                   CLIENT_CODE_MATCHES_SCHEMA)
    server_matches = server_matches.withColumn('id', F.ucase('id'))

    logger.info(f'Inserting any missing matches on the webserver not found in the delta table {server_matches.count()}')

    client_matches_dt.alias('target').merge(server_matches.alias('sccm'), 'target.id = sccm.id')\
        .whenNotMatchedInsertAll().execute()

    logger.info(f'Inserted all missing records for sync {query_params} from the server; count is now: {client_matches_dt.toDF().count()}')

# COMMAND ----------

def add_missing_server_matches(client_matches_dt: DeltaTable):
    logger.info(f'Found more records in Delta than the server, synchronizing...')

    server_ids_df = get_server_ids()

    if server_ids_df.count() > 0:
        missing_matches_df = (
            client_matches_dt.toDF()
            .withColumn('id', F.ucase('id'))
            .alias('sccm')
            .join(server_ids_df, on=['id'], how='leftanti')
        )
    else:
        missing_matches_df = client_matches_dt.toDF()

    # upload missing matches
    send_to_rest_service_if_applicable(httpx_client=HTTPX_CLIENT, df=missing_matches_df)

    logger.info('Sync done...')


# COMMAND ----------

def sync_databases(spark_session: SparkSession):
    logger.info('Syncing DeltaTable to SQL Database...')

    client_matches_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)

    logger.info('Handling any broken matches that may have been performed...')

    prism_dt = DeltaTable.forName(spark_session, PRISM_DATA_TABLE)

    handle_merge_event(spark_session, client_matches_dt=client_matches_dt, prism_dt=prism_dt)

    logger.info('Updating any matches found on server in delta table...')

    update_matches(spark_session, client_matches_dt)

    delta_size = client_matches_dt.toDF().count()

    logger.info('Comparing record sizes (Delta <-> SQL Server)...')

    # compare sizes
    count_url = f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches/search/count'
    headers = { 'Authorization': f'Bearer {RIVENDELL_TOKEN_RETRIEVER(HTTPX_CLIENT, CIP_CLIENT_ID, CIP_CLIENT_SECRET, AUTH_SERVER, RIVENDELL_AUTH_SCOPES)}' }

    logger.info('Getting server total count...')
    server_count = int(HTTPX_CLIENT.get(count_url, headers=headers).text)

    logger.info(f'Found {server_count} records on the server')
    logger.info(f'Found {delta_size} records in the Delta Table')

    if delta_size < server_count:
        dt_log_msg = (
            'DeltaTable is behind the Webserver, something likely happened to '
            'the DeltaTable as we never delete rows from the webserver unless '
            'delete endpoints are directly invoked from matching engine'
        )

        logger.info(dt_log_msg)
        logger.info(f'Delta: {delta_size}, Server: {server_count}')

        adding_processed_server_records(spark_session,
                                        client_matches_dt=client_matches_dt,
                                        query_params='matchType=CONFIRMED&matchType=MANUAL&matchType=IGNORED&matchType=POTENTIAL')
    elif delta_size > server_count:
        logger.info(f'Delta: {delta_size}, Server: {server_count}')

        add_missing_server_matches(client_matches_dt)

# COMMAND ----------

def create_tables_if_not_exist(spark_session: SparkSession) -> None:
    logger.info('Creating tables (if required)...')

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRISM_LAST_MODIFIED_TABLE} (
            last_modified TIMESTAMP
        ) USING DELTA
    """)

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRISM_DATA_TABLE} (
            account_id STRING NOT NULL,
            account_name STRING NOT NULL,
            account_type STRING,
            account_subtype STRING,
            account_status STRING NOT NULL,
            account_parent_id STRING,
            account_address_1 STRING,
            account_address_2 STRING,
            account_address_3 STRING,
            account_city STRING,
            account_state STRING,
            account_zipcode STRING,
            account_country STRING,
            account_region STRING,
            account_website STRING,
            account_tier STRING,
            account_short_code STRING,
            alias_name STRING,
            account_parent_name STRING,
            created_by STRING,
            cusip_id STRING,
            ownership_status STRING,
            is_parent STRING,
            investorType STRING,
            filing_date STRING,
            ownership_struct STRING,
            banking_account_tier STRING,
            sector_name STRING,
            classification_2 STRING,
            merged_with STRING,
            merged_accounts ARRAY<STRUCT<merged_account_id: STRING, merged_time: STRING>>
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true,
        delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite
        = true)
    """)

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {SUBORDINATE_DATA_TABLE} (
            client_mapping_source STRING NOT NULL,
            originating_source STRING NOT NULL,
            client_code STRING NOT NULL,
            client_display_name STRING,
            cv_cad DECIMAL(18, 2),
            cv_usd DECIMAL(18, 2),
            trade_date DATE
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true,
        delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite
        = true)
    """)

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_CODE_MATCHES_TABLE} (
            id STRING NOT NULL,
            ops_user_id LONG NOT NULL,
            account_id STRING NOT NULL,
            parent_name STRING,
            account_name STRING NOT NULL,
            account_type STRING,
            account_subtype STRING,
            account_status STRING NOT NULL,
            client_display_name STRING,
            client_code STRING NOT NULL,
            client_mapping_source STRING NOT NULL,
            originating_source STRING NOT NULL,
            client_value_cad DECIMAL(18, 2),
            client_value_usd DECIMAL(18, 2),
            comment STRING,
            matching_percentage FLOAT NOT NULL,
            matching_category STRING NOT NULL,
            match_type STRING NOT NULL,
            trade_date DATE,
            creation_time TIMESTAMP,
            update_time TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true,
        delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite
        = true)
    """)

    # The migrated managed tables were written with saveAsTable, so they do not carry the CDF
    # property and CREATE ... IF NOT EXISTS above will not add it. The change-feed checks
    # (breaking Prism changes, CV/name/trade-date/parent-name updates) all depend on it.
    # Only ALTER when missing: each ALTER adds a commit and requires table ownership (or MANAGE).
    for table_name in CDF_TABLES:
        cdf_enabled = (
            spark_session.sql(f"SHOW TBLPROPERTIES {table_name} ('delta.enableChangeDataFeed')")
            .first()['value']
        )

        if str(cdf_enabled).lower() != 'true':
            logger.info(f'Enabling change data feed on {table_name}')

            spark_session.sql(f"""
                ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
            """)

    logger.info('Table creation step done')

# COMMAND ----------

@retry(exceptions=(ConcurrentAppendException, ConcurrentTransactionException,
    ConcurrentDeleteReadException, ConcurrentDeleteDeleteException),
       delay=int(os.getenv(DELTA_MERGE_DELAY, "5")),
       tries=int(os.getenv(DELTA_MERGE_RETRIES, "10")),
       max_delay=int(os.getenv(DELTA_MERGE_MAX_DELAY, "60")),
       backoff=int(os.getenv(DELTA_MERGE_BACKOFF, "2")),
       jitter=(1, 10))
def merge_prism_data(spark_session: SparkSession, prism_data: List[Tuple[str, ...]]) -> None:
    if not prism_data:
        return

    delta_table = DeltaTable.forName(spark_session, PRISM_DATA_TABLE)

    # here we change the state to a status column
    # (once we get the prism data this will change)
    # blow up if Prism data is not there
    existing_records = spark_session.createDataFrame(prism_data, PRISM_SCHEMA)

    prism_update_setter = to_prism_sql(PRISM_FIELDS, 'prism_raw', ['account_id'])
    prism_insert_setter = to_prism_sql(PRISM_FIELDS, 'prism_raw', [])

    # we update all prism records we have a copy of if the ids are the same
    delta_table.alias('stored').merge(
        existing_records.alias('prism_raw'),
        'stored.account_id = prism_raw.account_id'
    ).whenMatchedUpdate(
        set=prism_update_setter
    ).whenNotMatchedInsert(
        values=prism_insert_setter
    ).execute()

# COMMAND ----------

def consolidate_prism_accts_if_required(spark_session: SparkSession) -> None:
    logger.info('Checking to see if any mergers have happened in Prism...')

    prism_df = spark_session.table(PRISM_DATA_TABLE)
    merge_cond = F.array_size(F.col('merged_accounts')) >= 1

    winner_accounts_df = prism_df.filter(F.when(merge_cond, F.lit(True)).otherwise(F.lit(False)))
    winner_accounts_df = (
        winner_accounts_df
        .withColumn('loser_accounts', F.transform('merged_accounts', lambda x: x.merged_account_id))
        .withColumn('loser_account_id', F.explode('loser_accounts'))
        .withColumnRenamed('account_id', 'winner_account_id')
        .drop('loser_accounts')
    )

    if winner_accounts_df.isEmpty():
        logger.info('No merged accounts found in Prism Delta table moving on')

        return

    process_cond = (
        (winner_accounts_df.loser_account_id == prism_df.account_id)
    )

    has_losers_df = winner_accounts_df.alias('mp').join(prism_df.alias('pd'), on=process_cond)

    if has_losers_df.isEmpty():
        logger.info('No loser accounts found in Prism Delta table moving on')

        return

    client_matches_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)
    client_df = client_matches_dt.toDF()
    client_df = client_df.select('id', 'account_id', 'client_display_name').alias('cf')

    # get all the loser accounts with the winner information
    # after the merge below, this invalidates the winner_accounts_df
    mergers_to_process = (
        winner_accounts_df.alias('mr')
        .join(client_df, on=(winner_accounts_df.loser_account_id == client_df.account_id))
        .withColumn('winner_account_name', F.col('account_name'))
        .withColumn('winner_account_type', F.col('account_type'))
        .withColumn('winner_account_subtype', F.col('account_subtype'))
        .withColumn('winner_account_status', F.col('account_status'))
        .withColumn('winner_parent_name', F.col('account_parent_name'))
    )

    matching_lower_bound = float(os.getenv(MATCHING_LOWER_BOUND, '80.0'))

    mergers_to_process = (
        mergers_to_process
        .withColumn('normalized_client_display_name', normalized_column('client_display_name'))
        .withColumn('normalized_prism_account_name', normalized_column('winner_account_name'))
        .withColumn('matching_percentage',
            match_terms('normalized_client_display_name', 'normalized_prism_account_name')) # type: ignore
        .withColumn('matching_category',
            F.when(F.col('matching_percentage') >= 90, 'HIGH')
                .when(F.col('matching_percentage') >= matching_lower_bound, 'MEDIUM')
                .otherwise('LOW')
        )
        .drop('normalized_client_display_name', 'normalized_prism_account_name')
    )

    records = [to_account_merge_record(r) for r in mergers_to_process.collect() if r['id']]

    logger.info(f'Consolidating server records with winner accounts...  {len(records)}')

    # update server first
    patch_server_by_id(httpx_client=HTTPX_CLIENT, records=records)

    logger.info(f'Consolidating Delta records with winner accounts...  {len(records)}')

    losers = spark_session.createDataFrame(winner_accounts_df.collect(), schema=winner_accounts_df.schema)

    cond = 'original.id = mergers.id'

    # update delta table
    client_matches_dt.alias('original')\
        .merge(mergers_to_process.alias('mergers'), condition=cond)\
        .whenMatchedUpdate(set={
            'account_id': 'mergers.winner_account_id',
            'account_name': 'mergers.winner_account_name',
            'account_type': 'mergers.winner_account_type',
            'account_subtype': 'mergers.winner_account_subtype',
            'account_status': 'mergers.winner_account_status',
            'parent_name': 'mergers.winner_parent_name',
            'matching_category': 'mergers.matching_category',
            'matching_percentage': 'mergers.matching_percentage'
        }).execute()

    prism_dt = DeltaTable.forName(spark_session, PRISM_DATA_TABLE)

    logger.info(f'Removing loser accounts from Prism DF copy {losers.count()}')

    prism_dt.alias('original')\
        .merge(losers.alias('losers'), 'original.account_id = losers.loser_account_id')\
        .whenMatchedDelete().execute()

    merge_result = get_last_merge_metrics(spark_session, CLIENT_CODE_MATCHES_TABLE)

    logger.info(f'ClientCodeMatches results {merge_result}')

    merge_result = get_last_merge_metrics(spark_session, PRISM_DATA_TABLE)

    logger.info(f'PrismData results {merge_result}')

    logger.info(f"Performed auto merge for local delta table (prism_data and client_code_matches)...")

def check_for_breaking_prism_changes(spark_session: SparkSession, prism_merge_version) -> DataFrame:
    cols = ['account_id', 'account_type', 'account_subtype']

    # FIX: inspect the Prism MERGE commit itself. The latest version could be the consolidation
    # loser-delete MERGE or an OPTIMIZE commit, which hid breaking changes.
    change_feed_df, pivot_df = _change_pivot(cols, PRISM_DATA_TABLE, prism_merge_version,
                                             filter_cond="_change_type like 'update_%'")

    # no changes
    if change_feed_df.count() == 0:
        logger.info('No breaking changes found during PRISM merge')

        schema = StructType([
            StructField("account_id", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("account_subtype", StringType(), True),
            StructField("update_postimage",
                        StructType([
                            StructField("account_type", StringType(), True),
                            StructField("account_subtype", StringType(), True)
                        ]))
        ])

        return spark.createDataFrame([], schema)

    logger.info('Breaking changes in PRISM merge detected...')

    # has any changes of interest?
    # we're interested in:
    # 1- Activations from non-active states (informational only)
    # 2- Active states going non-active (we need to break matches)
    # 3- Outright deletions of accounts (we need to break matches)
    combined_condition = (
        f"((update_preimage.account_type in {PRISM_ACCOUNT_TYPE_IN_FILTER} and "
        f"update_postimage.account_type not in {PRISM_ACCOUNT_TYPE_IN_FILTER}) or "
        f"(update_preimage.account_subtype in {PRISM_ACCOUNT_SUBTYPE_IN_FILTER} and "
        f"update_postimage.account_subtype not in {PRISM_ACCOUNT_SUBTYPE_IN_FILTER}))"
    )

    deactivated_df = pivot_df.filter(combined_condition)

    # no accounts deactivated or deleted accounts
    # nothing to do here
    if deactivated_df.count() == 0:
        logger.info('Changes found are not breaking (likely a field update that is immaterial), moving on')

        return spark.createDataFrame([], 'account_id: string, account_type: string, account_subtype: string')

    deactivated_df = deactivated_df.select('account_id',
                                          F.col('update_postimage.account_type').alias('account_type'),
                                          F.col('update_postimage.account_subtype').alias('account_subtype'))
    log_message = (
        f'Found this many changed accounts: {deactivated_df.count()}: '
    )

    logger.info(log_message)

    return deactivated_df

# COMMAND ----------

def handle_prism_breaking_changes(spark_session: SparkSession,
                                 breaking_changes_df: DataFrame) -> None:
    # hard and soft deletes
    if breaking_changes_df.count() == 0:
        logger.info(f'No breaking changes in PRISM')

        return

    logger.info('Handling breaking changes...')

    client_matches_df = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)

    logger.info('Removing breaking changes from Delta matches')

    # break matches (this will be in the CDF history)
    client_matches_df.alias('cmf')\
        .merge(breaking_changes_df.alias('br'), 'cmf.account_id = br.account_id')\
        .whenMatchedDelete().execute()

    batch_delete_url = f"{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/by-prism-account-id-list"

    logger.info('Deleting breaking changes from server')

    # delete from account_id batches
    delete_server_by_id(HTTPX_CLIENT, [r['account_id'] for r in breaking_changes_df.collect()], batch_delete_url)

    logger.info('PRISM breaking changes handled completely (done)')

# COMMAND ----------

def remove_potential_matches_on_name_change(spark_session: SparkSession, subordinate_merge_version) -> None:
    logger.info('Looking for changes in client display names')

    cols = ['client_code', 'originating_source', 'client_mapping_source']

    subordinate_df, pivot_df = _change_pivot(cols, SUBORDINATE_DATA_TABLE, subordinate_merge_version,
                                             filter_cond="_change_type like 'update_%'")

    if subordinate_df.isEmpty():
        logger.info('No client display names to update, moving on')

        return

    pivot_df = (
        pivot_df
        .filter('NOT (update_preimage.client_display_name <=> update_postimage.client_display_name)')
        .select(*cols)
        .distinct()
    )

    if pivot_df.isEmpty():
        logger.info('Nothing found in pivot, moving on')

        return

    logger.info('Getting name matches to update...')

    client_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)
    client_df = client_dt.toDF().filter('match_type = "POTENTIAL"')

    join_cond = (
        (pivot_df.client_code == client_df.client_code) &
        (pivot_df.originating_source == client_df.originating_source) &
        (pivot_df.client_mapping_source == client_df.client_mapping_source)
    )

    names_to_update = (
        client_df.join(pivot_df, on=join_cond)
            .select('id')
    )

    if names_to_update.count() == 0:
        logger.info(f'Did not find any existing potential matches for this shortcode/system with name changes, moving on')

        return

    ids_to_delete = [n.id.upper() for n in names_to_update.collect()]

    logger.info(f'Found {len(ids_to_delete)} this many records we are deleting from the server now')

    delete_server_by_id(HTTPX_CLIENT, ids_to_delete,
                       f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-delete')

    logger.info(f'Removing existing POTENTIAL matches from delta table now...')

    client_dt.alias('om')\
        .merge(names_to_update.alias('cm'), 'om.id = cm.id')\
        .whenMatchedDelete().execute()

    logger.info(f'Records will be rematched with new name... old name purged')

# COMMAND ----------

@retry(exceptions=(ConcurrentAppendException, ConcurrentTransactionException,
    ConcurrentDeleteReadException, ConcurrentDeleteDeleteException),
       delay=int(os.getenv(DELTA_MERGE_DELAY, "5")),
       tries=int(os.getenv(DELTA_MERGE_RETRIES, "10")),
       max_delay=int(os.getenv(DELTA_MERGE_MAX_DELAY, "60")),
       backoff=int(os.getenv(DELTA_MERGE_BACKOFF, "2")),
       jitter=(1, 10))
def handle_all_dates(spark_session: SparkSession, all_dates_df: DataFrame) -> None:
    logger.info('Reading subordinate Delta table to compare with all_dates...')

    match_condition = (
        'target.client_code = source.counterparty_code and '
        'target.originating_source = source.source_system'
    )
    update_condition = (
        'NOT (target.client_display_name <=> source.raw_client_name) '
        'or NOT (target.cv_usd <=> source.cv_usd) or NOT (target.trade_date <=> source.trade_date)'
    )

    insert_setters = {
        'client_mapping_source': F.lit(CLIENT_MAPPING_SOURCE),
        'originating_source': F.col('source.source_system'),
        'client_code': F.col('source.counterparty_code'),
        'client_display_name': F.col('source.raw_client_name'),
        'cv_cad': F.col('source.cv_cad'),
        'cv_usd': F.col('source.cv_usd'),
        'trade_date': F.col('source.trade_date')
    }
    update_setters = {
        'client_display_name': 'source.raw_client_name',
        'cv_usd': 'source.cv_usd',
        'cv_cad': 'source.cv_cad',
        'trade_date': 'source.trade_date'
    }

    logger.info(f'Merging subordinate data with all_dates...')

    pre_merge_version = _latest_version(SUBORDINATE_DATA_TABLE)

    args = (
        spark_session, SUBORDINATE_DATA_TABLE, all_dates_df
    )

    with merge_in_transaction(*args,
                              roll_back_on_hard_error=True,
                              delete_if_source_not_matched=True,
                              outer_alias_name='target',
                              inner_alias_name='source',
                              field_setters=update_setters,
                              field_inserts=insert_setters,
                              condition_for_matched=match_condition,
                              when_matched_update_condition=update_condition) as subordinate_dt:
        subordinate_merge_version = _merge_version_after(SUBORDINATE_DATA_TABLE, pre_merge_version)

        logger.info(f'Subordinate MERGE version: {subordinate_merge_version}')
        logger.info('Update CV if required')

        update_cv_if_changed(spark_session, subordinate_merge_version)

        logger.info('Updating client display names if required')

        remove_potential_matches_on_name_change(spark_session, subordinate_merge_version)

        logger.info('Removing potential matches no longer in subordinate data')

        remove_deleted_accounts(HTTPX_CLIENT, spark_session,
                                subordinate_dt.toDF(), ['client_code',
                                                        'originating_source', 'client_mapping_source'])

    logger.info('Merge complete...')

# COMMAND ----------

def update_cv_if_changed(spark_session: SparkSession, subordinate_merge_version) -> None:
    logger.info('Looking for changes in CV USD')

    cols = ['client_code', 'originating_source', 'client_mapping_source']

    subordinate_df, pivot_df = _change_pivot(cols, SUBORDINATE_DATA_TABLE, subordinate_merge_version,
                                             filter_cond="_change_type like 'update_%'")

    if subordinate_df.isEmpty():
        logger.info('No CV update, moving on')

        return

    pivot_df = (
        pivot_df
        .filter('update_preimage.cv_usd <> update_postimage.cv_usd')
        .withColumn('updated_cv_usd', F.col('update_postimage.cv_usd'))
        .select('updated_cv_usd', *cols)
        .distinct()
    )

    if pivot_df.isEmpty():
        logger.info('Nothing found in pivot, moving on')

        return

    logger.info('Getting CV matches to update...')

    client_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)
    client_df = client_dt.toDF().filter('match_type = "POTENTIAL"')

    join_cond = (
        (pivot_df.client_code == client_df.client_code) &
        (pivot_df.originating_source == client_df.originating_source) &
        (pivot_df.client_mapping_source == client_df.client_mapping_source)
    )

    cvs_to_update = (
        client_df.join(pivot_df, on=join_cond)
            .select('id', 'updated_cv_usd')
    )

    updates_for_server = [to_cv_patch_record(r) for r in cvs_to_update.collect()]

    logger.info(f'HTTP PATCHing server with records: {len(updates_for_server)}')

    patch_server_by_id(httpx_client=HTTPX_CLIENT, records=updates_for_server)

    logger.info(f'CVs updating in matches table')

    client_dt.alias('target')\
        .merge(cvs_to_update.alias('source'), 'target.id = source.id')\
        .whenMatchedUpdate(set={ 'client_value_usd': 'source.updated_cv_usd' })\
        .execute()

    logger.info(f'CVs updated')

# COMMAND ----------

def check_for_prism_differences(spark_session: SparkSession, prism_df: DataFrame,
                                prism_data: List[Tuple[str, ...]]) -> Tuple[bool, DataFrame]:
    existing_records = spark_session.createDataFrame(prism_data, PRISM_SCHEMA)
    existing_records = (
        existing_records
            .withColumnRenamed('account_id', 'prism_account_id')
            .withColumnRenamed('account_type', 'prism_account_type')
            .withColumnRenamed('account_subtype', 'prism_account_subtype')
            .withColumnRenamed('account_name', 'prism_account_name')
            .withColumnRenamed('account_status', 'prism_account_status')
    )
    existing_records = existing_records.select('prism_account_id', 'prism_account_type',
                                               'prism_account_name', 'prism_account_subtype',
                                               'prism_account_status')

    logger.info("Checking for changes in account type, subtype, status, or name")

    update_cond = (
        (prism_df.account_id == existing_records.prism_account_id) &
        (
            (~F.equal_null(prism_df.account_type, existing_records.prism_account_type)) |
            (~F.equal_null(prism_df.account_subtype, existing_records.prism_account_subtype)) |
            (~F.equal_null(prism_df.account_status, existing_records.prism_account_status)) |
            (~F.equal_null(prism_df.account_name, existing_records.prism_account_name))
        )
    )

    prism_updated_records_df = prism_df.join(existing_records, on=update_cond)

    return prism_updated_records_df.count() > 0, prism_updated_records_df

# COMMAND ----------

def rematch_changes_if_any(server_ccm_df: DataFrame) -> DataFrame:
    prism_derived_df = server_ccm_df.select(
        'id',
        'account_id',
        'account_name',
        'account_type',
        'account_subtype',
        'account_status'
    )

    prism_derived_df = (
        prism_derived_df
            .withColumnRenamed('id', 'external_id')
            .withColumnRenamed('account_id', 'prism_account_id')
            .withColumnRenamed('account_name', 'prism_account_name')
            .withColumnRenamed('account_type', 'prism_account_type')
            .withColumnRenamed('account_subtype', 'prism_account_subtype')
            .withColumnRenamed('account_status', 'prism_account_status')
    )

    rematched_df = rematch(prism_derived_df, with_actual_pct=True)

    if rematched_df.count() == 0:
        return server_ccm_df

    logger.info('Rematch found differences, merging to server results...')

    logger.info('Updating server match fields...')

    records = [to_match_patch_record(r) for r in rematched_df.collect() if r['id']]

    patch_server_by_id(httpx_client=HTTPX_CLIENT, records=records)

    rematched_df = (
        rematched_df
            .select('id', 'matching_category', 'matching_percentage')
            .withColumnRenamed('matching_category', 'new_ct')
            .withColumnRenamed('matching_percentage', 'new_pt')
    )

    updated_records = (
        server_ccm_df.alias('server')
            .join(rematched_df.alias('rematched'), on='id')
            .withColumn('matching_category',
                F.when(F.coalesce(F.trim(F.col("new_ct")), F.lit("")) != "",
                       F.col('new_ct')).otherwise(F.col('matching_category')))
            .withColumn('matching_percentage',
                F.when(F.coalesce(F.trim(F.col("new_pt")), F.lit("")) != "",
                       F.col('new_pt')).otherwise(F.col('matching_percentage')))
            .drop('new_ct')
            .drop('new_pt')
    )

    logger.info('Updating server records with new matches')

    return (
        server_ccm_df.join(updated_records,
                          on='id',
                          how='leftanti'
                          )
            .unionByName(updated_records).distinct()
    )

# COMMAND ----------

def rematch(prism_updated_records_df: DataFrame, with_actual_pct=False):
    logger.info(f'Rematching records...')
    client_matches_df = DeltaTable.forName(spark, CLIENT_CODE_MATCHES_TABLE).toDF()

    match_condition = (
        (
            (client_matches_df.account_id == prism_updated_records_df.prism_account_id) |
            (client_matches_df.id == prism_updated_records_df.external_id)
        ) &
        (
            (~F.equal_null(client_matches_df.account_name, prism_updated_records_df.prism_account_name)) |
            (~F.equal_null(client_matches_df.account_type, prism_updated_records_df.prism_account_type)) |
            (~F.equal_null(client_matches_df.account_subtype, prism_updated_records_df.prism_account_subtype)) |
            (~F.equal_null(client_matches_df.account_status, prism_updated_records_df.prism_account_status))
        )
    )

    matches_df = client_matches_df.join(prism_updated_records_df, on=match_condition)

    # Happens when the user:
    # 1- User overrides via Edit button
    # 2- User overrides via upload
    # 3- User overrides via merge
    edit_override = (
        matches_df
            .withColumn('matching_percentage', F.lit(100.0))
            .withColumn('matching_category', F.lit('HIGH'))
    )

    logger.info(f'Matching percentage set to 100% provisionally for override...')

    # option to perform actual match
    if with_actual_pct:
        logger.info(f'Performing actual rematch for Prism name changes...')

        matching_lower_bound = float(os.getenv(MATCHING_LOWER_BOUND, '80.0'))

        edit_override = (
            matches_df
                .withColumn('normalized_client_display_name', normalized_column('client_display_name'))
                .withColumn('normalized_prism_account_name', normalized_column('prism_account_name'))
                .withColumn('matching_percentage',
                    match_terms('normalized_client_display_name', 'normalized_prism_account_name'))
                .withColumn('matching_category',
                    F.when(F.col('matching_percentage') >= 90, 'HIGH')
                        .when(F.col('matching_percentage') >= matching_lower_bound, 'MEDIUM')
                        .otherwise('LOW')
                )
                .drop('normalized_client_display_name')
                .drop('normalized_prism_account_name')
        )

    edit_override = edit_override.drop('external_id')

    return edit_override.drop_duplicates()


# COMMAND ----------

def update_matches_on_server_if_required(spark_session: SparkSession,
                                        prism_data: List[Tuple[str, ...]]) -> None:
    prism_df = DeltaTable.forName(spark_session, PRISM_DATA_TABLE).toDF()

    if not prism_data or prism_df.count() == 0:
        return

    has_changes, prism_updated_records_df = check_for_prism_differences(spark_session, prism_df, prism_data)

    if not has_changes:
        stmt = (
            "PRISM records fields of interest still the same on the server, "
            "nothing to update on webserver matches"
        )
        logger.info(stmt)

        return

    logger.info("Changes found. Now looking to existing matching records to update match percentage if name changed on PRISM side")

    prism_updated_records_df = prism_updated_records_df.withColumn('external_id', F.lit('##N/A##'))

    matches_df = rematch(prism_updated_records_df, with_actual_pct=True)

    if matches_df.isEmpty():
        logger.info('No PRISM changes found to update, not touching the webserver or match delta table')

        return

    logger.info(f"Updating webserver records with PRISM changes...  changes: {matches_df.count()}")

    records = [to_match_patch_record(r) for r in matches_df.collect() if r['id']]

    patch_server_by_id(httpx_client=HTTPX_CLIENT, records=records)

    client_matches_dt = DeltaTable.forName(spark, CLIENT_CODE_MATCHES_TABLE)

    logger.info(f'Updating local delta table with {matches_df.count()}...')

    client_matches_dt.alias('original')\
        .merge(matches_df.alias('updated'), condition='original.id = updated.id')\
        .whenMatchedUpdate(set={
            'matching_percentage': 'updated.matching_percentage',
            'matching_category': 'updated.matching_category',
            'account_name': 'updated.prism_account_name',
            'account_status': 'updated.prism_account_status',
            'account_type': 'updated.prism_account_type',
            'account_subtype': 'updated.prism_account_subtype'
        }).execute()

    merge_result = get_last_merge_metrics(spark_session, CLIENT_CODE_MATCHES_TABLE)
    logger.info(f'ClientCodeMatches results {merge_result}')
    logger.info("Updates done on webserver/delta table to match PRISM changes...")

# COMMAND ----------

@retry(exceptions=(ConcurrentAppendException, ConcurrentTransactionException,
    ConcurrentDeleteReadException, ConcurrentDeleteDeleteException),
       delay=int(os.getenv(DELTA_MERGE_DELAY, "5")),
       tries=int(os.getenv(DELTA_MERGE_RETRIES, "10")),
       max_delay=int(os.getenv(DELTA_MERGE_MAX_DELAY, "60")),
       backoff=int(os.getenv(DELTA_MERGE_BACKOFF, "2")),
       jitter=(1, 10))
def handle_non_matching_delete(spark_session: SparkSession, results_df: DataFrame, join_fields: List[str]) -> None:
    log_message = (
        'Performing any required non-match deletes (this is for client '
        'codes previously not matching any PRISM records but now do)...'
    )
    logger.info(log_message)

    results_df = results_df.dropDuplicates(join_fields)
    results_df = results_df.withColumn('join_id', F.concat(*join_fields))

    client_matches_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)
    client_matches_df = client_matches_dt.toDF().where(f'account_id = "{NON_MATCHING_PRISM_ID}"')
    client_matches_df = client_matches_df.withColumn('original_id', F.col('id'))

    logger.info(f'Found {client_matches_df.count()} matches for matches with dummy account {NON_MATCHING_PRISM_ID}')

    records_to_delete_df = (
        client_matches_df.alias('matches')
            .join(results_df.alias('results'), on=['client_code', 'originating_source', 'client_mapping_source'])
    )

    log_message = (
        'Deleting any old all_dates matches not having a corresponding '
        'PRISM match but now does...'
    )

    logger.info(log_message)

    # save records before they're invalidated by the cache manager
    ids_to_delete = [r['original_id'].upper() for r in records_to_delete_df.select('original_id').collect()]

    client_matches_dt.alias('om')\
        .merge(records_to_delete_df.alias('cm'), 'om.id = cm.original_id')\
        .whenMatchedDelete().execute()

    logger.info('Looking for any deletes performed by the merge (if any)')
    logger.info(f'Total to delete: {len(ids_to_delete)}')

    # any deletions should be reflected upstream
    if ids_to_delete:
        logger.info(f'Found {len(ids_to_delete)} this many records we are deleting from the server now')

        delete_server_by_id(HTTPX_CLIENT, ids_to_delete,
                           f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-delete')
# COMMAND ----------

@retry(exceptions=(ConcurrentAppendException, ConcurrentTransactionException),
       delay=int(os.getenv(DELTA_MERGE_DELAY, "5")),
       tries=int(os.getenv(DELTA_MERGE_RETRIES, "10")),
       max_delay=int(os.getenv(DELTA_MERGE_MAX_DELAY, "60")),
       backoff=int(os.getenv(DELTA_MERGE_BACKOFF, "2")),
       jitter=(1, 10))
def handle_missing_prism_matches(spark_session: SparkSession, non_matched_clients_df: DataFrame):
    logger.info('Handling codes with no matches in PRISM')

    non_matched_clients_df = non_matched_clients_df.drop('join_id')

    # create dummy client matches to send to the front end
    non_matched_clients_df = (
        non_matched_clients_df
        .withColumn('ops_user_id', F.lit(SYSTEM_ID).cast('long'))
        .withColumn('parent_name', F.lit(UNMATCHED))
        .withColumn('account_name', F.lit(UNMATCHED))
        .withColumn('account_id', F.lit(NON_MATCHING_PRISM_ID))
        .withColumn('account_type', F.lit(UNMATCHED))
        .withColumn('account_subtype', F.lit(UNMATCHED))
        .withColumn('account_status', F.lit(UNMATCHED))
        .withColumn('matching_percentage', F.lit(0))
        .withColumn('matching_category', F.lit('LOW'))
        .withColumn('match_type', F.lit('POTENTIAL'))
        .withColumn('comment', F.lit('no match found'))
        .withColumn('client_value_usd', F.col('cv_usd'))
        .withColumn('client_value_cad', F.col('cv_cad'))
        .withColumn('trade_date', F.col('trade_date'))
    )

    non_matched_clients_df = non_matched_clients_df.drop('cv_cad').drop('cv_usd')
    non_matched_clients_df = non_matched_clients_df.distinct()
    non_matched_clients_df = non_matched_clients_df.dropDuplicates([
        'client_mapping_source',
        'originating_source',
        'client_code',
        'account_id'
    ])

    client_matches_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)
    non_matched_clients_df = non_matched_clients_df.withColumn('creation_time',
                                                            F.current_timestamp())
    non_matched_clients_df = non_matched_clients_df.withColumn('id',
                                                            generate_uuid_str())

    # this df comes from an anti-join against the client_code_matches table so
    # after the merge, Spark can silently invalidate non_matched_clients_df
    # The .cache() should prevent recomputation, but Delta Lake write operations
    # can invalidate Spark's in-memory cache for DataFrames that read from the
    # modified table, causing it to be recomputed from scratch after the merge.
    non_matched_clients_df_rows = non_matched_clients_df.collect()
    non_matched_clients_df = (
        spark_session.createDataFrame(
            non_matched_clients_df_rows,
            non_matched_clients_df.schema
        )
    )

    logger.info('Storing dummy matches for codes with no PRISM matches (if any new ones found)')

    merge_cond = (
        'em.client_code = ndm.client_code and '
        'em.originating_source = ndm.originating_source and '
        'em.client_mapping_source = ndm.client_mapping_source'
    )

    client_matches_dt.alias('em')\
        .merge(non_matched_clients_df.alias('ndm'), condition=merge_cond)\
        .whenNotMatchedInsert(values={
            'id': 'ndm.id',
            'ops_user_id': 'ndm.ops_user_id',
            'client_mapping_source': 'ndm.client_mapping_source',
            'originating_source': 'ndm.originating_source',
            'client_code': 'ndm.client_code',
            'client_display_name': 'ndm.client_display_name',
            'client_value_cad': 'ndm.client_value_cad',
            'client_value_usd': 'ndm.client_value_usd',
            'trade_date': 'ndm.trade_date',
            'account_id': 'ndm.account_id',
            'parent_name': 'ndm.parent_name',
            'account_name': 'ndm.account_name',
            'account_type': 'ndm.account_type',
            'account_subtype': 'ndm.account_subtype',
            'account_status': 'ndm.account_status',
            'match_type': 'ndm.match_type',
            'matching_percentage': 'ndm.matching_percentage',
            'matching_category': 'ndm.matching_category',
            'comment': 'ndm.comment',
            'creation_time': 'ndm.creation_time'
        }).execute()

    return non_matched_clients_df

# COMMAND ----------

def get_stop_words(httpx_client: Client):
    try:
        stop_words = get_server_record(httpx_client,
                                      f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/stop-words?size=100', 'stopWordses',
                                      lambda row: (row['word'], row['ngrams']))
        stop_words = sorted(stop_words, key=lambda x: x[1], reverse=True)
        stop_words = [r[0].lower() for r in stop_words]

        stop_words.extend(StopWordsRemover().getStopWords())

        return '|'.join([r.lower() for r in stop_words])
    except Exception:
        logger.exception("Failed to get stop words from server, defaulting to internal list")

        return STOP_WORDS_AS_REGEX

# COMMAND ----------

def get_saved_new_matches(spark_session, matched_results_df: DataFrame, join_fields:
                          List[str]) -> DataFrame:
    logger.info('Getting rid of duplicate matches...')

    matched_results_df = matched_results_df.dropDuplicates(join_fields)
    matched_results_df = matched_results_df.withColumn('join_id', F.concat(*join_fields))

    logger.info('Getting existing Client Matches...')

    existing_client_matches_df = spark_session.table(CLIENT_CODE_MATCHES_TABLE)

    # create merged id
    existing_client_matches_df = existing_client_matches_df.withColumn('join_id', F.concat(*join_fields))

    ops_processed_matches = (
        existing_client_matches_df.withColumn('confirm_id',
                                              F.concat(*join_fields[:-1]))
            .filter('match_type in ("CONFIRMED", "IGNORED", "MANUAL")')
    )

    logger.info('Filtering out matches already proposed or existing...')

    matched_results_df = (
        matched_results_df.alias('matched')
            .join(existing_client_matches_df.alias('existing'), on=['join_id'], how='leftanti')
    )

    matched_results_df = matched_results_df.withColumn('confirm_id',
                                                      F.concat(*join_fields[:-1]))

    logger.info('Filtering out matches already confirmed for client codes...')

    matched_results_df = (
        matched_results_df.alias('matched')
            .join(ops_processed_matches.alias('existing'), on=['confirm_id'], how='leftanti')
    )

    matched_results_df = matched_results_df.drop('join_id')
    matched_results_df = matched_results_df.drop('confirm_id')

    logger.info('Caching filtered matched results...')

    matched_results_df.write.format('delta').mode('overwrite').option("mergeSchema", "true") \
        .saveAsTable(MATCHED_RESULTS_TABLE)

    if matched_results_df.is_cached:
        matched_results_df.unpersist()

    # FIX: append the persisted snapshot, not the lazy DataFrame. Re-evaluating the lazy plan
    # re-reads client_code_matches and re-runs any id-generating UDF, so the rows appended to
    # Delta could differ (count or ids) from the rows returned here and sent to the server.
    persisted_results_df = spark_session.table(MATCHED_RESULTS_TABLE)
    new_match_count = persisted_results_df.count()

    if new_match_count > 0:
        logger.info(f'Found new matches {new_match_count}, updating delta table...')

        persisted_results_df.write.format('delta').mode('append').saveAsTable(CLIENT_CODE_MATCHES_TABLE)
    else:
        logger.info(f'No new matches found')

    # Added to deal with weird situation where DF goes to zero
    return spark_session.table(MATCHED_RESULTS_TABLE)

# COMMAND ----------

def cherry_pick_results(spark_session: SparkSession, matched_results_df: DataFrame, join_fields: List[str]) -> DataFrame:
    matched_results_df = get_saved_new_matches(spark_session, matched_results_df, join_fields)

    logger.info('Looking for any client codes with no matches in PRISM...')

    subordinate_df = spark.table(SUBORDINATE_DATA_TABLE)
    subset_join_fields = F.concat(*join_fields[:-1])

    subordinate_df = subordinate_df.withColumn('join_id', subset_join_fields)

    existing_client_matches_df = spark_session.table(CLIENT_CODE_MATCHES_TABLE)

    existing_client_matches_df = (
        existing_client_matches_df
            .withColumn('join_id', subset_join_fields)
    )

    # get client codes with no matches and create dummy match on screen for
    # display (if any)
    non_matched_clients_df = (
        subordinate_df.alias('sub')
            .filter(~F.col('originating_source').eqNullSafe('CALYPSO'))
            .join(existing_client_matches_df.alias('ex'), on=['join_id'], how='leftanti')
    )

    if non_matched_clients_df.count() == 0:
        logger.info('No client codes found NOT having a corresponding PRISM match')

        return matched_results_df

    non_matched_clients_df = handle_missing_prism_matches(spark_session, non_matched_clients_df)
    non_matched_clients_df = non_matched_clients_df.dropDuplicates(join_fields)

    return matched_results_df.unionByName(non_matched_clients_df)

# COMMAND ----------

def update_latest_trade_date(spark_session: SparkSession, client_dt: DeltaTable,
                             subordinate_df: DataFrame):
    if client_dt.toDF().isEmpty():
        return

    cond = 'ct.client_code = ad.client_code and ct.originating_source = ad.originating_source'

    pre_merge_version = _latest_version(CLIENT_CODE_MATCHES_TABLE)

    client_dt.alias('ct').merge(subordinate_df.alias('ad'), cond)\
        .whenMatchedUpdate(
            condition='NOT (ct.trade_date <=> ad.trade_date)',
            set={
                'trade_date': 'ad.trade_date'
            }
        ).execute()

    cols = ['id', 'client_mapping_source', 'client_code', 'originating_source']

    merge_version = _merge_version_after(CLIENT_CODE_MATCHES_TABLE, pre_merge_version)

    _, pivot_df = _change_pivot(cols, CLIENT_CODE_MATCHES_TABLE, merge_version,
                                filter_cond="_change_type like 'update_%'")

    combined_condition = f"NOT (update_preimage.trade_date <=> update_postimage.trade_date)"

    new_trade_date_df = pivot_df.filter(combined_condition)

    if new_trade_date_df.count() > 0:
        logger.info('Got some trade_date updates, updating server')

        new_trade_date_df = new_trade_date_df.withColumn('trade_date', F.col('update_postimage.trade_date'))
        records = [to_trade_date_update(r) for r in new_trade_date_df.collect() if r['id']]

        patch_server_by_id(httpx_client=HTTPX_CLIENT, records=records)

# COMMAND ----------

def update_latest_prism_parent_name(spark_session: SparkSession, client_dt: DeltaTable):
        if client_dt.toDF().isEmpty():
            return

        prism_df = spark_session.table(PRISM_DATA_TABLE)

        pre_merge_version = _latest_version(CLIENT_CODE_MATCHES_TABLE)

        client_dt.alias('ct').merge(prism_df.alias('prism'), 'ct.account_id = prism.account_id')\
            .whenMatchedUpdate(
                condition='NOT (ct.parent_name <=> prism.account_parent_name)',
                set={
                    'parent_name': 'prism.account_parent_name'
                }
            ).execute()

        cols = ['id', 'account_id']

        merge_version = _merge_version_after(CLIENT_CODE_MATCHES_TABLE, pre_merge_version)

        _, pivot_df = _change_pivot(cols, CLIENT_CODE_MATCHES_TABLE, merge_version,
                                    filter_cond="_change_type like 'update_%'")

        new_parent_name_df = pivot_df.filter("NOT (update_preimage.parent_name <=> update_postimage.parent_name)")

        if new_parent_name_df.count() > 0:
            logger.info('Got some parent name updates, updating server')

            new_parent_name_df = new_parent_name_df.withColumn('parent_name',
                                                             F.col('update_postimage.parent_name'))

            records = [to_parent_name_update(r) for r in new_parent_name_df.collect() if r['id']]

            patch_server_by_id(httpx_client=HTTPX_CLIENT, records=records)

# COMMAND ----------

def update_client_code_extras(spark_session: SparkSession) -> None:
    logger.info('Updating extra fields for client_code_matches (if any need it)')

    subordinate_df = spark.table(SUBORDINATE_DATA_TABLE)

    client_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)

    if client_dt.toDF().isEmpty():
        return

    # FIX: restore to the version captured before each step. rollback_merge() restored the
    # last MERGE - 1, which undid the *previous* (successful) step when a step failed before
    # its own MERGE committed.
    pre_step_version = _latest_version(CLIENT_CODE_MATCHES_TABLE)

    try:
        update_latest_trade_date(spark_session=spark_session, client_dt=client_dt, subordinate_df=subordinate_df)
    except Exception as ex:
        logger.exception(f'Exception encountered whilst trying to update trade dates... {ex}')

        if _latest_version(CLIENT_CODE_MATCHES_TABLE) > pre_step_version:
            client_dt.restoreToVersion(pre_step_version)

        raise

    pre_step_version = _latest_version(CLIENT_CODE_MATCHES_TABLE)

    try:
        update_latest_prism_parent_name(spark_session=spark_session, client_dt=client_dt)
    except Exception as ex:
        logger.exception(f'Exception encountered whilst trying to update parent names... {ex}')

        if _latest_version(CLIENT_CODE_MATCHES_TABLE) > pre_step_version:
            client_dt.restoreToVersion(pre_step_version)

        raise

# COMMAND ----------

def calculate_weights(data: DataFrame, ngram_count: int, input_label: str,
                      output_label: str) -> OrderedDict:
    if data.count() == 0:
        return OrderedDict()

    tokenizer = Tokenizer(inputCol='account_name', outputCol='tokens')
    t_transform = tokenizer.transform(data)

    ngram = NGram(n=ngram_count, inputCol='tokens', outputCol=input_label)
    n_transform = ngram.transform(t_transform)

    counter = CountVectorizer(inputCol=input_label, outputCol=output_label)
    cv_model = counter.fit(n_transform)
    cv_df = cv_model.transform(n_transform)

    idf = IDF(inputCol=output_label, outputCol='idf')
    idf_model = idf.fit(cv_df)

    return OrderedDict(zip(cv_model.vocabulary, idf_model.idf.toArray()))

# COMMAND ----------

def update_prism_latest_datetime(spark_session, prism_data):
    if not prism_data:
        return

    timestamp = datetime.now() - timedelta(days=1)
    last_modified = timestamp.strftime(PYTHON_DATETIME_FORMAT)

    logger.info(f'Updating last modified timestamp to: {last_modified}')

    # Watermark is a managed Delta table under UC (was a path-based parquet table).
    # Always written as exactly one row: the previous rewrite of existing rows left the
    # watermark unchanged if the table was ever empty.
    df = (
        spark_session.createDataFrame([(last_modified,)], ['last_modified'])
        .withColumn('last_modified',
                    F.to_timestamp('last_modified', SPARK_DATETIME_FORMAT))
    )

    df.write.format('delta').mode('overwrite').saveAsTable(PRISM_LAST_MODIFIED_TABLE)


def publish_prism_account_feed(spark_session) -> None:
    spark_session.sql("""
        CREATE OR REPLACE VIEW raw.prism_account_feed AS
        WITH mapped_accounts AS (
            SELECT
                account_id,
                MAX(
                    CASE
                        WHEN UPPER(originating_source) IN ('COWEN', 'US_PRIME')
                        THEN TRUE
                        ELSE FALSE
                    END
                ) AS is_equity_trading_account,
                MAX(update_time) AS last_mapping_update
            FROM raw.client_code_matches
            WHERE account_id IS NOT NULL
                AND UPPER(match_type) IN ('CONFIRMED', 'MANUAL')
            GROUP BY account_id
        )
        SELECT
            p.account_id AS PRISM_ACCOUNT_ID,
            p.account_name AS PRISM_ACCOUNT_NAME,
            m.is_equity_trading_account AS IS_EQUITY_TRADING_ACCOUNT,
            COALESCE(m.last_mapping_update, CURRENT_TIMESTAMP()) AS LAST_UPDATED_DATETIME
        FROM raw.prism_data p
        INNER JOIN mapped_accounts m
            ON p.account_id = m.account_id
    """)

# COMMAND ----------

logger.info('Starting CIP Matching Engine')

spark.conf.set("spark.sql.ansi.enabled", "false")

logger.info(f"""
    Trigger Params:
    TRIGGER_NAME = {TRIGGER_NAME}
    TRIGGER_START_TIME = {TRIGGER_START_TIME}
    TRIGGER_TYPE = {TRIGGER_TYPE}
    TRIGGER_SCHEDULE = {TRIGGER_SCHEDULE}
""")

create_tables_if_not_exist(spark_session=spark)

sync_databases(spark_session=spark)

logger.info('Getting PRISM data')

prism_data = call_prism_actual(HTTPX_CLIENT, PRISM_WEB_SERVER_BASE_URL,
                              spark_session=spark,
                              location=PRISM_LAST_MODIFIED_TABLE)

logger.info('Checking for duplicate PRISM ids (will fail if encountered)')

check_for_duplicates(prism_server_data=prism_data)

logger.info('No duplicates found, looking for updates in Prism data and applying to server')

update_matches_on_server_if_required(spark_session=spark, prism_data=prism_data)

logger.info('PRISM retrieval done... merging with Delta Prism records...')

prism_pre_merge_version = _latest_version(PRISM_DATA_TABLE)

merge_prism_data(spark_session=spark, prism_data=prism_data)

prism_merge_version = _merge_version_after(PRISM_DATA_TABLE, prism_pre_merge_version)

logger.info(f'Prism MERGE version: {prism_merge_version}')

logger.info('Performing Prism consolidation (merger) if required')

consolidate_prism_accts_if_required(spark_session=spark)

logger.info('Removing potential matches for accounts no longer in Prism')

prism_clause = f'match_type = "POTENTIAL" and account_id <> "{NON_MATCHING_PRISM_ID}"'

remove_deleted_accounts(HTTPX_CLIENT, spark,
                        spark.table(PRISM_DATA_TABLE), ['account_id'],
                        filter_clause=prism_clause)

logger.info('PRISM Delta merge completed... Checking for any breaking changes, i.e. Prism account inactive or deleted')

breaking_changes_df = check_for_breaking_prism_changes(spark_session=spark,
                                                       prism_merge_version=prism_merge_version)

handle_prism_breaking_changes(spark_session=spark, breaking_changes_df=breaking_changes_df)

update_prism_latest_datetime(spark, prism_data=prism_data)

logger.info('Reading all_dates data...')

all_dates_df = construct_all_dates(spark_session=spark)

handle_all_dates(spark_session=spark, all_dates_df=all_dates_df)

logger.info('Applying server updates for trade dates and parent names where applicable')

update_client_code_extras(spark)

logger.info('Reading active PRISM accounts for match')

prism_df = (
    spark.table(PRISM_DATA_TABLE)
        .filter(f"account_type in {PRISM_ACCOUNT_TYPE_IN_FILTER} and account_subtype in {PRISM_ACCOUNT_SUBTYPE_IN_FILTER}")
)

logger.info('Reading subordinate accounts for match')

subordinate_df = spark.table(SUBORDINATE_DATA_TABLE)

subordinate_df = (
    subordinate_df.filter(~F.col('originating_source').eqNullSafe('CALYPSO'))
)

logger.info('Getting stop words from server')

stop_words = get_stop_words(HTTPX_CLIENT)

logger.info('Performing preprocessing of PRISM accounts for matching...')

prism_df = preprocess_for_match(df=prism_df,
                               display_name_column='account_name',
                               stop_words_as_regex=stop_words)

logger.info('Performing preprocessing of ALL_DATES accounts for matching...')

subordinate_df = preprocess_for_match(df=subordinate_df,
                                     display_name_column='client_display_name',
                                     stop_words_as_regex=stop_words)

logger.info('Performing match...')

results_df = perform_match(subordinate_df=subordinate_df, master_df=prism_df, stop_words_as_regex=stop_words)

logger.info('Matching transformation complete (not distributed execution), looking for non-matches in the past that now have a match')

join_fields = ['client_mapping_source',
               'originating_source',
               'client_code',
               'account_id',
               ]

handle_non_matching_delete(spark, results_df, join_fields=join_fields)

logger.info('Getting new matches and cherry picking results (also handling non-matches)')

cherry_picked_df = cherry_pick_results(spark_session=spark, matched_results_df=results_df, join_fields=join_fields)
cherry_picked_df = cherry_picked_df.dropDuplicates(join_fields)

logger.info(f'Saving records (if any) to the server: {cherry_picked_df.count()}')

send_to_rest_service_if_applicable(httpx_client=HTTPX_CLIENT, df=cherry_picked_df)

logger.info('Matching engine completed...')
logger.info('Storing computed IDF weights...')

prism_df = (
    spark.table(PRISM_DATA_TABLE)
        .filter(f"account_type in {PRISM_ACCOUNT_TYPE_IN_FILTER} and account_subtype in {PRISM_ACCOUNT_SUBTYPE_IN_FILTER}")
)

weights = calculate_weights(prism_df, 2, 'bigram_data', 'bigram_counts')
weights.update(calculate_weights(prism_df, 1, 'singlegram_data', 'singlegram_counts'))

if not weights:
    logger.info('No weights found exiting...')

else:
    try:
        detected_words = [{
                            'word': w[0],
                            'idfScore': w[1],
                            'ngrams': len(w[0].split(' '))
                            }
                            for w in weights.items()]

        payload = {'detectedWords': detected_words}
        headers = { 'Authorization': f'Bearer {RIVENDELL_TOKEN_RETRIEVER(HTTPX_CLIENT, CIP_CLIENT_ID, CIP_CLIENT_SECRET, AUTH_SERVER, RIVENDELL_AUTH_SCOPES)}' }

        logger.info('Deleting older weights...')

        HTTPX_CLIENT.delete(url=f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/detected-stop-words/delete-all', headers=headers)

        logger.info('Inserting weights...')

        HTTPX_CLIENT.post(url=f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/detected-stop-words/insert-all',
                          json=payload, headers=headers)
    except Exception as e:
        error_msg = (
            "Problem occurred during persisted of detected "
            "words... not critical to matching engine but something that "
            "should be looked at"
        )

        logger.exception(error_msg)

# FIX: was inside the else-branch, so the view was skipped whenever no weights were found.
publish_prism_account_feed(spark)

HTTPX_CLIENT.close()

logger.info('Done')

# COMMAND ----------

# End of Notebook