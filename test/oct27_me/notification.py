# Databricks notebook source
# MAGIC %md
# MAGIC ## Matching Engine - TrueMatch Diff Notification (Unity Catalog)
# MAGIC Captures account / client-name changes on CONFIRMED and MANUAL matches (from the
# MAGIC `raw.client_code_matches` change feed, plus Prism / all_dates deletions) into
# MAGIC `raw.truematch_account_diffs`, and publishes the new diffs to the server for user acknowledgement.
# MAGIC
# MAGIC The stored `max_version`, `prism_max_version` and `subordinate_max_version` columns are change-feed
# MAGIC watermarks. They are validated against the UC tables' version history before use.

# COMMAND ----------

from typing import Tuple
import os
import json
import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
    ArrayType,
    TimestampType
)
from delta.tables import DeltaTable
from httpx import Client, Limits, Timeout

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

struct = F.struct('account_id', 'account_name', 'account_type', 'account_subtype',
                  'account_status', 'client_display_name', 'parent_name', 'client_code',
                  'originating_source', 'match_type', 'commit_timestamp')

image_schema = StructType([
    StructField('account_id', StringType()),
    StructField('account_name', StringType()),
    StructField('account_type', StringType()),
    StructField('account_subtype', StringType()),
    StructField('account_status', StringType()),
    StructField('client_display_name', StringType()),
    StructField('parent_name', StringType()),
    StructField('client_code', StringType()),
    StructField('originating_source', StringType()),
    StructField('match_type', StringType())
])

schema = StructType([
    StructField('match_id', StringType()),
    StructField('commit_version', LongType()),
    StructField('common_fields', image_schema),
    StructField('diff', ArrayType(StructType([
        StructField('column_name', StringType()),
        StructField('before', StringType()),
        StructField('after', StringType())
    ]))),
    StructField('max_version', LongType()),
    StructField('was_prism_deleted', BooleanType()),
    StructField('was_all_dates_deleted', BooleanType()),
    StructField('did_prism_change', BooleanType()),
    StructField('did_all_dates_change', BooleanType())
])


outer_schema = StructType([
    StructField('match_id', StringType()),
    StructField('commit_version', LongType()),
    StructField('before_client_display_name', StringType()),
    StructField('after_client_display_name', StringType()),
    StructField('before_account_name', StringType()),
    StructField('after_account_name', StringType()),
    StructField('update_preimage', image_schema),
    StructField('update_postimage', image_schema),
    StructField('did_acct_id_change', BooleanType()),
    StructField('did_acct_parent_name_change', BooleanType()),
    StructField('did_acct_name_change', BooleanType()),
    StructField('did_acct_type_change', BooleanType()),
    StructField('did_acct_subtype_change', BooleanType()),
    StructField('did_acct_status_change', BooleanType()),
    StructField('did_client_display_name_change', BooleanType()),
    StructField('was_prism_deleted', BooleanType()),
    StructField('was_all_dates_deleted', BooleanType()),
    StructField('max_version', LongType()),
    StructField('commit_timestamp', TimestampType()),
    StructField('diff', schema)
])


# COMMAND ----------

dbutils.widgets.text('cip_client_id', '')
dbutils.widgets.text('cip_client_secret', '')
dbutils.widgets.text('client_matching_web_server_base_url', '')
dbutils.widgets.text('auth_server', '')
dbutils.widgets.text('true_match_locations', '')
dbutils.widgets.text('auth_and_date_formats', '')
dbutils.widgets.text('CATALOG', '')

CATALOG_NAME = dbutils.widgets.get('CATALOG').strip()

if not CATALOG_NAME:
    raise ValueError('CATALOG widget must be set (e.g. d4001-centralus-tdsci-cipcatalog).')

# Set before the %run utilities below so any table they resolve uses this catalog.
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./access_token_util.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_constants.py"

# COMMAND ----------

# MAGIC %run "./network_utils.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils.py"

# COMMAND ----------

# MAGIC %run "./matching_engine_server_methods.py"

# COMMAND ----------

# DBTITLE 1,Unity Catalog tables (managed) and version helpers
TRUEMATCH_DIFFS_TABLE = 'raw.truematch_account_diffs'
CLIENT_CODE_MATCHES_TABLE = 'raw.client_code_matches'
PRISM_DATA_TABLE = 'raw.prism_data'
SUBORDINATE_DATA_TABLE = 'raw.subordinate_data'


def _latest_version(table_name: str) -> int:
    return DeltaTable.forName(spark, table_name).history(1).select('version').first()['version']


def _max_merge_version(table_name: str):
    row = (
        DeltaTable.forName(spark, table_name).history()
        .filter(F.lower(F.col('operation')) == 'merge')
        .agg(F.max('version').alias('max_merge_version'))
        .first()
    )
    return row['max_merge_version']


def _validate_watermark(label: str, stored_version, table_name: str) -> None:
    """Fail fast if a stored change-feed watermark cannot belong to the current table.

    The UC migration re-created the source tables, so their version history restarted at 0.
    Watermarks captured against the Hive Metastore tables are then higher than any UC version,
    and reading the change feed from them would fail or silently skip/replay changes.
    """
    if stored_version is None:
        return

    latest = _latest_version(table_name)

    if stored_version > latest:
        raise ValueError(
            f'{label}={stored_version} stored in {TRUEMATCH_DIFFS_TABLE} is ahead of {table_name} '
            f'(latest version {latest}). The table history was reset by the Unity Catalog migration; '
            f'run the TrueMatch watermark re-baseline before this notebook.'
        )

# COMMAND ----------

def create_table_if_required(spark_session: SparkSession) -> None:
    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {TRUEMATCH_DIFFS_TABLE} (
                match_id STRING,
                commit_version LONG,
                before_client_display_name STRING,
                after_client_display_name STRING,
                before_account_name STRING,
                after_account_name STRING,
                account_id STRING,
                did_acct_id_change BOOLEAN,
                did_acct_parent_name_change BOOLEAN,
                did_acct_name_change BOOLEAN,
                did_acct_type_change BOOLEAN,
                did_acct_subtype_change BOOLEAN,
                did_acct_status_change BOOLEAN,
                did_client_display_name_change BOOLEAN,
                was_prism_deleted BOOLEAN,
                was_all_dates_deleted BOOLEAN,
                max_version LONG,
                prism_max_version LONG,
                subordinate_max_version LONG,
                commit_timestamp TIMESTAMP,

                diff STRUCT<
                        match_id: STRING,
                        commit_version: LONG,
                        common_fields: STRUCT<
                                account_id: STRING,
                                account_name: STRING,
                                account_type: STRING,
                                account_subtype: STRING,
                                account_status: STRING,
                                client_display_name: STRING,
                                parent_name: STRING,
                                client_code: STRING,
                                originating_source: STRING,
                                match_type: STRING
                        >,
                        diff: ARRAY<STRUCT<
                                column_name: STRING,
                                before: STRING,
                                after: STRING
                        >>,
                        max_version: LONG,
                        was_prism_deleted: BOOLEAN,
                        was_all_dates_deleted: BOOLEAN,
                        did_prism_change: BOOLEAN,
                        did_all_dates_change: BOOLEAN
                >
        )
        USING DELTA
        TBLPROPERTIES (
                delta.enableChangeDataFeed = true,
                delta.autoOptimize.autoCompact = true,
                delta.autoOptimize.optimizeWrite = true
        )
    """)

# COMMAND ----------

def remove_unmatched_diffs(spark_session: SparkSession, client: Client):
    # distinct: the diffs table holds one row per match per captured commit
    diff_dt = spark_session.table(TRUEMATCH_DIFFS_TABLE).select('match_id').distinct()
    client_dt = spark_session.table(CLIENT_CODE_MATCHES_TABLE)

    cond = (
        (client_dt.id == diff_dt.match_id) &
        (client_dt.match_type.isin(['POTENTIAL']))
    )

    removed_df = (
        client_dt.alias('ct')
            .join(diff_dt.alias('dt'), on=cond)
    )

    if removed_df.isEmpty():
        logger.info('No match diffs to delete')

        return

    ids = removed_df.collect()
    url = f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-match-diff/'
    records = []

    logger.info(f'Get impacted records {len(ids)}')

    for rec in ids:
        rec_url = f'{url}search/findByMatchId?matchId={rec.match_id}'

        headers = {
            'Authorization': f'Bearer {RIVENDELL_TOKEN_RETRIEVER(client, CIP_CLIENT_ID, CIP_CLIENT_SECRET, AUTH_SERVER, RIVENDELL_AUTH_SCOPES)}'
        }

        response = client.get(rec_url, headers=headers)
        response.raise_for_status()

        data = response.json()
        # An empty HAL collection may omit "_embedded" entirely.
        diffs = [str(d['id']) for d in (data.get('_embedded') or {}).get('clientCodeMatchDiffChanges', [])]

        records.extend(diffs)

    records = set(records)

    if not records:
        logger.info('No match ids found on the server to delete, moving on')

        return

    logger.info(f'Deleting non-matching diffs from server {len(records)}')

    for id_ in records:
        delete_url = url + id_

        headers = {
            'Authorization': f'Bearer {RIVENDELL_TOKEN_RETRIEVER(client, CIP_CLIENT_ID, CIP_CLIENT_SECRET, AUTH_SERVER, RIVENDELL_AUTH_SCOPES)}'
        }

        response = client.delete(delete_url, headers=headers)

        # FIX: delete failures were ignored and the local rows were removed anyway, leaving
        # orphan diffs on the server that were never retried. 404 = already deleted (a retry).
        if response.status_code != 404:
            response.raise_for_status()

    logger.info('Deleting non-matching diffs from Delta Table')

    # Targeted delete instead of rewriting the whole table with mode('overwrite').
    removed_ids_df = removed_df.select('match_id').distinct()

    DeltaTable.forName(spark_session, TRUEMATCH_DIFFS_TABLE).alias('tf')\
        .merge(removed_ids_df.alias('rf'), 'tf.match_id = rf.match_id')\
        .whenMatchedDelete()\
        .execute()

# COMMAND ----------

def create_deleted_df(spark_session: SparkSession, prism_df: DataFrame, c360_df: DataFrame, struct: Column, prism_from_version: int,
                      subordinate_from_version: int) -> DataFrame:
    client_code_matches_df = (
        spark_session.table(CLIENT_CODE_MATCHES_TABLE)
        .filter(F.col('match_type').isin(['CONFIRMED', 'MANUAL']))
    )

    client_code_matches_df = (
        client_code_matches_df.withColumn('cpty_id',
                                         F.concat(F.col('client_code'),
                                         F.col('originating_source')))
            .withColumn('did_acct_id_change', F.lit(False))
            .withColumn('did_acct_parent_name_change', F.lit(False))
            .withColumn('did_acct_name_change', F.lit(False))
            .withColumn('did_acct_type_change', F.lit(False))
            .withColumn('did_acct_subtype_change', F.lit(False))
            .withColumn('did_acct_status_change', F.lit(False))
            .withColumn('did_client_display_name_change', F.lit(False))
            .withColumn('was_prism_deleted', F.lit(False))
            .withColumn('was_all_dates_deleted', F.lit(False))
            .withColumn('commit_version', F.lit(-1))
            .withColumn('commit_timestamp', F.lit(None))
            .withColumn('update_preimage', struct)
            .withColumn('update_postimage',struct)
            .withColumnRenamed('id', 'match_id')
    )

    prism_deleted_df = (
        client_code_matches_df.alias('deleted')
            .join(prism_df.alias('prism'),
                  on=(F.col('deleted.account_id') == F.col('prism.account_id')), how='leftanti')
            .withColumn('was_prism_deleted', F.lit(True))
            .drop('prism.account_id')
    )

    prism_history_df = get_history_df(spark_session, PRISM_DATA_TABLE, ['delete'], version=prism_from_version)

    prism_deleted_df = (
        prism_deleted_df.alias('pdf')
            .join(prism_history_df.alias('hist').select(F.col('account_id'), F.col('_commit_timestamp').alias('delete_commit_timestamp')),
                  on=['account_id'], how='leftouter')
                       .withColumn('commit_timestamp', F.col('delete_commit_timestamp'))
            .drop('delete_commit_timestamp')
    )

    all_dates_deleted_df = (
        client_code_matches_df.alias('deleted')
            .join(c360_df.alias('all_dates'), on=['cpty_id'], how='leftanti')
            .withColumn('was_all_dates_deleted', F.lit(True))
    )

    all_dates_hist_df = get_history_df(spark_session, SUBORDINATE_DATA_TABLE, ['delete'], version=subordinate_from_version)

    all_dates_deleted_df = (
        all_dates_deleted_df.alias('adf')
            .join(all_dates_hist_df.alias('hist').select(F.col('client_code'),
                                                       F.col('originating_source'), F.col('_commit_timestamp').alias('delete_commit_timestamp')),
                  on=['client_code', 'originating_source'], how='leftouter')
                       .withColumn('commit_timestamp', F.col('delete_commit_timestamp'))
            .drop('delete_commit_timestamp')
    )

    deleted_all_dates_prism_df = all_dates_deleted_df.unionByName(prism_deleted_df)

    # collapse rows deleted by prism and all_dates
    window = Window.partitionBy(F.col('match_id'))

    drop_columns = ['count', "ops_user_id", "account_id", "account_name", "account_type",
                    "account_subtype", "account_status", "client_display_name", "client_code",
                    "client_mapping_source", "originating_source", "client_value_cad", "client_value_usd",
                    "comment", "matching_percentage", "matching_category", "match_type", "creation_time",
                    "update_time", "parent_name", "trade_date"]

    deleted_df = (
        deleted_all_dates_prism_df.withColumn('count', F.count('match_id').over(window))
            .withColumn('was_all_dates_deleted', F.when(F.col('count') > 1, F.lit(True)).otherwise(F.col('was_all_dates_deleted')))
            .withColumn('was_prism_deleted', F.when(F.col('count') > 1, F.lit(True)).otherwise(F.col('was_prism_deleted')))
            .drop(*drop_columns)
            .dropDuplicates(['match_id'])
    )
    max_prism_version = _max_merge_version(PRISM_DATA_TABLE)
    max_c360_version = _max_merge_version(SUBORDINATE_DATA_TABLE)

    return deleted_df, max_prism_version, max_c360_version

# COMMAND ----------

def non_null_condition(first_column_name: str, second_col_name: str) -> Column:
    return (
        (
            (F.coalesce(F.col(first_column_name), F.lit('')) != '') |
            (F.coalesce(F.col(second_col_name), F.lit('')) != '')
        ) &
        (~F.col(first_column_name).eqNullSafe(F.col(second_col_name)))
    )

def create_updated_df(spark_session: SparkSession, client_code_matches_df: DataFrame, prism_df: DataFrame,
                      c360_df: DataFrame, deleted_all_dates_prism_df: DataFrame, struct: Column) -> DataFrame:
    client_code_matches_df = (
        client_code_matches_df
            .filter(~F.col('_change_type').isin('insert', 'delete')) # take all match types here, filter later
                .withColumnRenamed('_commit_version', 'commit_version')
                .withColumnRenamed('_commit_timestamp', 'commit_timestamp')
                .withColumnRenamed('id', 'match_id')
            .orderBy('commit_version', 'match_id')
    )

    change_condition = (
        (~F.col('update_preimage.account_id').eqNullSafe(F.col('update_postimage.account_id'))) |
        (non_null_condition('update_preimage.account_name', 'update_postimage.account_name')) |
        (~F.col('update_preimage.account_type').eqNullSafe(F.col('update_postimage.account_type'))) |
        (~F.col('update_preimage.account_subtype').eqNullSafe(F.col('update_postimage.account_subtype'))) |
        (~F.col('update_preimage.account_status').eqNullSafe(F.col('update_postimage.account_status'))) |
        (non_null_condition('update_preimage.client_display_name', 'update_postimage.client_display_name')) |
        (non_null_condition('update_preimage.parent_name', 'update_postimage.parent_name'))
    )

    changes_df = (
        client_code_matches_df.groupBy('commit_version', 'match_id')
            .pivot('_change_type', ['update_preimage', 'update_postimage'])
            .agg(F.first(struct))
            .filter(change_condition)
            .filter((~F.col('update_preimage.account_id').eqNullSafe(NON_MATCHING_PRISM_ID)) & (F.col('update_postimage.match_type').isin('CONFIRMED', 'MANUAL')))
              # we only care about changes to confirmed/manual matches
            .withColumn('cpty_id', F.concat(F.col('update_postimage.client_code'), F.col('update_postimage.originating_source')))
            .withColumn('did_acct_id_change', ~F.col('update_preimage.account_id').eqNullSafe(F.col('update_postimage.account_id')))
            .withColumn('did_acct_parent_name_change', ~F.col('update_preimage.parent_name').eqNullSafe(F.col('update_postimage.parent_name')))
            .withColumn('did_acct_name_change', ~F.col('update_preimage.account_name').eqNullSafe(F.col('update_postimage.account_name')))
            .withColumn('did_acct_type_change', ~F.col('update_preimage.account_type').eqNullSafe(F.col('update_postimage.account_type')))
            .withColumn('did_acct_subtype_change', ~F.col('update_preimage.account_subtype').eqNullSafe(F.col('update_postimage.account_subtype')))
            .withColumn('did_acct_status_change', ~F.col('update_preimage.account_status').eqNullSafe(F.col('update_postimage.account_status')))
            .withColumn('did_client_display_name_change', ~F.col('update_preimage.client_display_name').eqNullSafe(F.col('update_postimage.client_display_name')))
            .withColumn('was_prism_deleted', F.lit(False))
            .withColumn('was_all_dates_deleted', F.lit(False))
            .withColumn('commit_timestamp', F.col('update_postimage.commit_timestamp'))
    )

    # Let's get all matches where the prism and all_dates accounts exist as opposed to
    # deleted_all_dates_prism_df where it strictly gets accounts that were deleted
    prism_matched_df = changes_df.alias('matched').join(prism_df.alias('prism'), on=(F.col('matched.update_postimage.account_id')
                                                                                == F.col('prism.account_id'))).drop('account_id')
    all_dates_matched_df = changes_df.alias('matched').join(c360_df.alias('all_dates'), on=['cpty_id'])

    # merge matches with prism and all_dates accounts
    changes_df = all_dates_matched_df.unionByName(prism_matched_df)

    # merge deleted accounts with active accounts
    changes_df = changes_df.unionByName(deleted_all_dates_prism_df).drop('cpty_id')

    changes_df = (
        changes_df.withColumn('before_client_display_name', F.col('update_preimage.client_display_name'))
                  .withColumn('after_client_display_name', F.col('update_postimage.client_display_name'))
                  .withColumn('before_account_name', F.col('update_preimage.account_name'))
                  .withColumn('after_account_name', F.col('update_postimage.account_name'))
                  .withColumn('account_id', F.col('update_postimage.account_id'))
    )

    true_match_df = spark_session.table(TRUEMATCH_DIFFS_TABLE)
    true_match_df = true_match_df.select('match_id', 'commit_version')

    # remove possible duplicates
    changes_df = (
        changes_df.alias('cf')
            .join(true_match_df.alias('tf'), on=['match_id', 'commit_version'], how='leftanti')
    )

    max_version = _max_merge_version(CLIENT_CODE_MATCHES_TABLE)

    logger.info(f'New max version: {max_version=}')

    return changes_df.withColumn('max_version',
                                F.lit(max_version).cast(LongType()))

# COMMAND ----------

@F.udf(returnType=schema)
def produce_diff(preimage, postimage, change_struct):
    def apply_diff(column_name, before_value, after_value):
        return {
            'column_name': column_name,
            'before': before_value,
            'after': after_value
        }

    diff_payload = {
        'match_id': change_struct.match_id,
        'commit_version': change_struct.commit_version,
        'max_version': change_struct.max_version,
        'commit_timestamp': postimage.commit_timestamp,
        'was_prism_deleted': change_struct.was_prism_deleted,
        'was_all_dates_deleted': change_struct.was_all_dates_deleted,
        'did_prism_change': False,
        'did_all_dates_change': False,
        'common_fields': {
            'account_id': postimage.account_id,
            'parent_name': postimage.parent_name if not change_struct.did_acct_parent_name_change else None,
            'account_name': postimage.account_name if not change_struct.did_acct_name_change else None,
            'account_type': postimage.account_type if not change_struct.did_acct_type_change else None,
            'account_subtype': postimage.account_subtype if not change_struct.did_acct_subtype_change else None,
            'account_status': postimage.account_status if not change_struct.did_acct_status_change else None,
            'client_display_name': postimage.client_display_name if not change_struct.did_client_display_name_change else None,
            'client_code': postimage.client_code,
            'originating_source': postimage.originating_source,
            'match_type': postimage.match_type
        },
        'diff': []
    }

    if change_struct.did_acct_id_change:
        diff_payload['diff'].append(apply_diff('account_id', preimage.account_id, postimage.account_id))
        diff_payload['did_prism_change'] = True
    if change_struct.did_acct_parent_name_change:
        diff_payload['diff'].append(apply_diff('parent_name', preimage.parent_name, postimage.parent_name))
        diff_payload['did_prism_change'] = True
    if change_struct.did_acct_name_change:
        diff_payload['diff'].append(apply_diff('account_name', preimage.account_name, postimage.account_name))
        diff_payload['did_prism_change'] = True
    if change_struct.did_acct_type_change:
        diff_payload['diff'].append(apply_diff('account_type', preimage.account_type, postimage.account_type))
        diff_payload['did_prism_change'] = True
    if change_struct.did_acct_subtype_change:
        diff_payload['diff'].append(apply_diff('account_subtype', preimage.account_subtype, postimage.account_subtype))
        diff_payload['did_prism_change'] = True
    if change_struct.did_acct_status_change:
        diff_payload['diff'].append(apply_diff('account_status', preimage.account_status, postimage.account_status))
        diff_payload['did_prism_change'] = True
    if change_struct.did_client_display_name_change:
        diff_payload['diff'].append(apply_diff('client_display_name', preimage.client_display_name, postimage.client_display_name))
        diff_payload['did_all_dates_change'] = True

    return diff_payload

# COMMAND ----------

logger.info('Capturing latest diff for client code matches')

changes_df = None
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

try:
    logger.info('Creating new table (if required) and getting latest version processed')

    create_table_if_required(spark)

    logger.info('Removing diffs no longer matched...')

    remove_unmatched_diffs(spark, HTTPX_CLIENT)

    # FIX: take the MAX of each watermark. Every append writes that run's versions on all of its
    # rows, so collect()[0] returned an arbitrary (usually older) run's watermark.
    stored = (
        spark.table(TRUEMATCH_DIFFS_TABLE)
            .agg(F.max('max_version').alias('max_version'),
                 F.max('prism_max_version').alias('prism_max_version'),
                 F.max('subordinate_max_version').alias('subordinate_max_version'))
            .first()
    )
    has_watermark = stored['max_version'] is not None

    _validate_watermark('max_version', stored['max_version'], CLIENT_CODE_MATCHES_TABLE)
    _validate_watermark('prism_max_version', stored['prism_max_version'], PRISM_DATA_TABLE)
    _validate_watermark('subordinate_max_version', stored['subordinate_max_version'], SUBORDINATE_DATA_TABLE)

    dt_versions = (
            DeltaTable.forName(spark, CLIENT_CODE_MATCHES_TABLE).history()
            .filter(F.lower(F.col('operation')) == 'merge')
            .select('version').orderBy('version').collect()
            )

    if not dt_versions:
        dbutils.notebook.exit("No changes detected, exiting")

    if has_watermark and dt_versions[-1].version == stored['max_version']:
        dbutils.notebook.exit("No changes detected, exiting")

    from_version = stored['max_version'] if has_watermark else dt_versions[0].version
    prism_from_version = (stored['prism_max_version'] or 0) if has_watermark else 0
    subordinate_from_version = (stored['subordinate_max_version'] or 0) if has_watermark else 0

    prism_df = spark.table(PRISM_DATA_TABLE).select('account_id')
    c360_df = spark.table(SUBORDINATE_DATA_TABLE)
    c360_df = c360_df.select(F.concat(c360_df.client_code, c360_df.originating_source).alias('cpty_id')).distinct()

    logger.info('Building deleted df transformation...')

    deleted_all_dates_prism_df, max_prism_version, max_c360_version = (
        create_deleted_df(spark_session=spark,
            prism_df=prism_df,
            c360_df=c360_df,
            struct=struct,
            prism_from_version=prism_from_version,
            subordinate_from_version=subordinate_from_version)
    )

    client_code_matches_df = read_cdf_across_boundaries(spark,
                                                      CLIENT_CODE_MATCHES_TABLE,
                                                      starting_version=from_version)

    logger.info(f'Versions picked up from delta tables: {from_version=}, {max_prism_version=}, {max_c360_version=}')

    changes_df = create_updated_df(spark_session=spark, client_code_matches_df=client_code_matches_df,
                                  prism_df=prism_df,
                                  c360_df=c360_df,
                                  deleted_all_dates_prism_df=deleted_all_dates_prism_df,
                                  struct=struct)
    changes_df = (
        changes_df.withColumn('prism_max_version', F.lit(max_prism_version).cast(LongType()))
            .withColumn('subordinate_max_version', F.lit(max_c360_version).cast(LongType()))
    )

    logger.info('Caching transformations...')
    changes_df.cache()

    change_struct = F.struct('match_id', 'max_version', 'commit_version', 'commit_timestamp', 'was_prism_deleted',
                             'was_all_dates_deleted',
                             'did_acct_id_change', 'did_acct_parent_name_change',
                             'did_acct_name_change', 'did_acct_type_change',
                             'did_acct_subtype_change', 'did_acct_status_change',
                             'did_client_display_name_change')

    logger.info('Producing diff between pre and post image...')

    diff_df = (
        changes_df.withColumn('diff', produce_diff(F.col('update_preimage'), F.col('update_postimage'), change_struct))
            .drop('update_preimage', 'update_postimage')
            .dropDuplicates()
    )

    logger.info('Saving diff to delta table...')

    # FIX: append the collected snapshot rather than re-evaluating the lazy plan, so the rows
    # stored locally are exactly the rows sent to the server.
    save_copy = spark.createDataFrame(diff_df.collect(), diff_df.schema)

    pre_append_version = _latest_version(TRUEMATCH_DIFFS_TABLE)

    save_copy.write.format('delta').mode('append').saveAsTable(TRUEMATCH_DIFFS_TABLE)

    logger.info('Saving to server for user acknowledgement...')

    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj): # type: ignore
            if isinstance(obj, (datetime)):
                return obj.isoformat()

            return super(DateTimeEncoder, self).default(obj)

    save_copy = save_copy.drop('prism_max_version', 'subordinate_max_version')

    try:
        save_diffs(HTTPX_CLIENT, save_copy, converter=lambda x: json.loads(DateTimeEncoder().encode(x)))
    except Exception:
        # FIX: previously the diffs stayed in Delta after a failed publish; the next run's
        # (match_id, commit_version) de-duplication then excluded them, so users never saw them.
        logger.exception(f'Publishing diffs failed; restoring {TRUEMATCH_DIFFS_TABLE} '
                         f'to version {pre_append_version}')

        DeltaTable.forName(spark, TRUEMATCH_DIFFS_TABLE).restoreToVersion(pre_append_version)

        raise

    logger.info('Done saving the diffs...')
finally:
    if changes_df and changes_df.is_cached:
        changes_df.unpersist()

    HTTPX_CLIENT.close()

# COMMAND ----------

# End of Notebook