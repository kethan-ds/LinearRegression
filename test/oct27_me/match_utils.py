# Databricks notebook source
# MAGIC %md
# MAGIC ## Matching Engine - Match Utilities (utility, %run only)
# MAGIC Matching transforms, all_dates preparation, and potential-match clean-up.
# MAGIC
# MAGIC **Unity Catalog:** all Delta access is by table name in the caller's current catalog (the caller
# MAGIC runs `USE CATALOG` before `%run`). Table names below can be overridden by defining them before
# MAGIC this notebook is `%run`.

# COMMAND ----------

import os
from typing import List
from datetime import datetime, timezone, timedelta

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import Column, DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable
from httpx import Client

# COMMAND ----------

# DBTITLE 1,Unity Catalog tables
if 'CLIENT_CODE_MATCHES_TABLE' not in globals():
    CLIENT_CODE_MATCHES_TABLE = 'raw.client_code_matches'

if 'ALL_DATES_TABLE' not in globals():
    # OPEN: confirm this is the table behind the HMS 'all_dates_curated_location' widget.
    ALL_DATES_TABLE = 'silver.all_dates'


def _is_uc_table_identifier(identifier: str) -> bool:
    # catalog.schema.table / schema.table are dotted with no path separator; paths contain '/'.
    return '.' in identifier and '/' not in identifier


def _delta_table(spark_session: SparkSession, identifier: str) -> DeltaTable:
    if _is_uc_table_identifier(identifier):
        return DeltaTable.forName(spark_session, identifier)
    return DeltaTable.forPath(spark_session, identifier)

# COMMAND ----------

def preprocess_for_match(df: DataFrame, display_name_column: str, stop_words_as_regex: str = STOP_WORDS_AS_REGEX) -> DataFrame:
    trim_lower_cases = F.trim(F.lcase(F.col(display_name_column)))
    remove_stop_words = F.regexp_replace(trim_lower_cases, rf'\b({stop_words_as_regex})\b', '')
    remove_excess_white_space = F.regexp_replace(remove_stop_words, r'\s+', ' ')
    extracted_columns_array = F.regexp_extract_all(remove_excess_white_space, regexp=F.lit(r'\w+'), idx=0)
    cleaned_array = F.array_remove(extracted_columns_array, '')
    sort_extracted = F.array_sort(cleaned_array)
    acronym = F.concat_ws('', F.transform(sort_extracted, lambda x: x.substr(0, 1)))

    df = (
        df.withColumn('extracted_terms', sort_extracted)
          .withColumn('matching_criteria_array', get_sound('extracted_terms')) # type: ignore
          .withColumn('matching_criteria_array',
                      F.array_union('matching_criteria_array', F.array(acronym)))
    )

    return df.withColumn('matching_key', F.explode('matching_criteria_array'))

def normalized_column(column_name: str, stop_words_as_regex: str = STOP_WORDS_AS_REGEX) -> Column:
    col = F.col(column_name)
    removed_non_words = F.regexp_replace(col, F.lit(r'[^\w\s]'), '')
    lower_case = F.lcase(removed_non_words)

    return F.regexp_replace(lower_case, F.lit(r'\s+'), ' ')

def perform_match(subordinate_df: DataFrame, master_df: DataFrame, stop_words_as_regex: str = STOP_WORDS_AS_REGEX) -> DataFrame:
    # Matches EVERYTHING again
    # later we decide what to keep
    # when we build the client_matches
    joined_df = (
        subordinate_df.alias('sub').join(master_df.alias('master'), on='matching_key')
            .select(
                F.lit(1).cast('long').alias('ops_user_id'),
                F.col('sub.client_mapping_source').alias('client_mapping_source'),
                F.col('sub.originating_source').alias('originating_source'),
                F.col('sub.client_code').alias('client_code'),
                F.col('sub.client_display_name').alias('client_display_name'),
                F.col('sub.cv_cad').alias('client_value_cad'),
                F.col('sub.cv_usd').alias('client_value_usd'),
                F.col('sub.trade_date').alias('trade_date'),
                F.col('master.account_parent_name').alias('parent_name'),
                F.col('master.account_name').alias('account_name'),
                F.col('master.account_id').alias('account_id'),
                F.col('master.account_type').alias('account_type'),
                F.col('master.account_subtype').alias('account_subtype'),
                F.col('master.account_status').alias('account_status'),
                F.lit('POTENTIAL').alias('match_type'),
                F.lit('potential auto match').alias('comment'))
            .distinct()
    )

    matching_lower_bound = float(os.getenv(MATCHING_LOWER_BOUND, '80.0'))

    results_df = (
        joined_df
            .withColumn('normalized_client_name', normalized_column('client_display_name', stop_words_as_regex))
            .withColumn('normalized_account_name', normalized_column('account_name', stop_words_as_regex))
            .withColumn('matching_percentage',
                        match_terms('normalized_client_name', 'normalized_account_name')) # type: ignore
            .withColumn('matching_category',
                        F.when(F.col('matching_percentage') >= 90, 'HIGH')
                            .when(F.col('matching_percentage') >= matching_lower_bound, 'MEDIUM')
                            .otherwise('LOW')
                        )
    )

    results_df = results_df.drop('normalized_client_name').drop('normalized_account_name')
    results_df = results_df.withColumn('creation_time', F.current_timestamp())
    results_df = results_df.withColumn('id', generate_uuid7_str())

    results_df = results_df.filter(f'matching_percentage >= {matching_lower_bound}')

    window = Window.partitionBy('client_mapping_source', 'originating_source', 'client_code')

    inactive_nonmatches_df = (
        results_df
            .withColumn('max', F.max('matching_percentage').over(window))
            .withColumn('join_key', F.concat('client_mapping_source',
                                           'originating_source',
                                           'client_code', 'account_id'))
            .filter(F.lower('account_status') == 'inactive')
            .filter(F.col('matching_percentage') < F.col('max'))
            .drop('max')
    )

    # remove inactive not having the top score
    results_df = (
        results_df
            .withColumn('join_key', F.concat('client_mapping_source',
                                           'originating_source',
                                           'client_code', 'account_id'))
            .join(inactive_nonmatches_df.select('join_key'),
                  on=['join_key'], how='leftanti')
            .drop('join_key')
    )

    # flag all groups with at least one active account with a max score
    results_df = (
        results_df
            .withColumn('max', F.max('matching_percentage').over(window))
            .withColumn('has_active_max',
                F.max(
                    F.when(
                        (F.lower('account_status') == 'active') &
                        (F.col('matching_percentage') == F.col('max')),
                        True
                    )
                ).over(window)
            )
            .drop('max')
    )

    # get inactive accounts with maxes having a competing active account
    false_positive_inactives_df = (
        results_df
            .filter(F.lower('account_status') == 'inactive')
            .filter(F.col('has_active_max'))
    )

    # remove false positives
    # OPEN (pre-existing, not changed): this anti-join is on account_id only, so an inactive
    # account flagged in ONE client-code group is removed from EVERY group's results.
    results_df = (
        results_df
            .join(false_positive_inactives_df.select('account_id'),
                  on=['account_id'], how='leftanti')
    )

    results_df = results_df.drop('has_active_max')

    results_df.cache()

    return results_df

# COMMAND ----------

def uuid7_to_column(col_name: str) -> Column:
    return (
        # FIX: first segment read the literal column 'id' instead of col_name
        F.lower(F.concat_ws('-', F.hex(F.substring(col_name, 1, 4)),
            F.hex(F.substring(col_name, 5, 2)),
            F.hex(F.substring(col_name, 7, 2)),
            F.hex(F.substring(col_name, 9, 2)),
            F.hex(F.substring(col_name, 11, 6))))
    )

def uuid7_str_to_binary(source_column: str) -> Column:
    return F.unhex(F.regexp_replace(F.col(source_column), '-', ''))

def get_latest_version(spark_session: SparkSession, delta_location: str) -> int:
    """Latest MERGE version. Accepts a UC table name (preferred) or a path."""
    dt = _delta_table(spark_session, delta_location)

    row = (
        dt.history()
            .filter(F.lower(F.col('operation')) == F.lit("merge"))
            .select('version')
            .orderBy('version', ascending=False)
            .first()
    )

    # FIX: .first().version raised AttributeError on a table with no MERGE yet
    # (e.g. a freshly migrated UC table).
    if row is None:
        raise ValueError(f'No MERGE found in the history of {delta_location}')

    return row.version

# COMMAND ----------

def all_dates_cv_calc(all_dates: DataFrame):
    today = datetime.now(timezone.utc)
    year_ago = today - timedelta(weeks=52)

    all_dates_agg = (
        all_dates
            .withColumn('today', F.lit(today))
            .withColumn('year_diff', F.lit(year_ago))
            .filter('trade_date between year_diff and today')
            .groupBy('source_system', 'counterparty_code')
            .agg(
                F.sum(F.col('client_value_usd')).cast(DecimalType(18, 2)).alias('trailing_cv_usd'),
                F.sum(F.col('client_value_cad')).cast(DecimalType(18, 2)).alias('trailing_cv_cad')
            )
            .drop('today')
            .drop('year_diff')
    )

    condition = (
        (F.col('original.source_system') == F.col('agg.source_system')) &
        (F.col('original.counterparty_code') == F.col('agg.counterparty_code'))
    )

    all_dates_aliased = all_dates.alias('original')
    all_dates_agg_aliased = all_dates_agg.alias('agg')

    return (
        all_dates_aliased.alias('original')
            .join(all_dates_agg_aliased.alias('agg'), on=condition, how='left_outer')
            .withColumn('client_value_cad', F.coalesce(F.col('trailing_cv_cad'), F.lit(0)))
            .withColumn('client_value_usd', F.coalesce(F.col('trailing_cv_usd'), F.lit(0)))
            .drop(F.col('agg.source_system'))
            .drop(F.col('agg.counterparty_code'))
            .drop('trailing_cv_cad')
            .drop('trailing_cv_usd')
    )


# COMMAND ----------

def clean_all_dates(spark_session: SparkSession, with_trailing_sum: bool =True) -> DataFrame:
    logger.info('Retrieving and cleaning all_dates..')

    all_dates_df = spark_session.table(ALL_DATES_TABLE)

    # clean up all_dates
    all_dates_df = (
        all_dates_df
            .withColumn('client_value_cad',
                        F.when(all_dates_df.client_value_cad.isin('null', ''), None)
                        .otherwise(all_dates_df.client_value_cad))
            .withColumn('client_value_usd',
                        F.when(all_dates_df.client_value_usd.isin('null', ''), None)
                        .otherwise(all_dates_df.client_value_usd))
                        .filter('is_client_facing = 1')
    )

    logger.info(f'Filtering out all_dates with NO counterparty_code before count: {all_dates_df.count()}')

    all_dates_df = all_dates_df.filter(
        F.coalesce(F.trim(F.col("counterparty_code")), F.lit("")) != ""
    )

    logger.info(f'Filtering all_dates with NO counterparty_code after count: {all_dates_df.count()}')
    logger.info(f'Filtering out all_dates with NO source_system before count: {all_dates_df.count()}')

    all_dates_df = all_dates_df.filter(
        F.coalesce(F.trim(F.col("source_system")), F.lit("")) != ""
    )
    all_dates_df = all_dates_df.drop('is_client_facing')

    if with_trailing_sum:
        # calc trailing cv
        all_dates_df = all_dates_cv_calc(all_dates_df)

    all_dates_df = (
        all_dates_df
            .withColumn('raw_client_name',
                F.when(F.coalesce(F.trim(F.col("raw_client_name")), F.lit("")) != "",
                F.trim(F.col("raw_client_name"))).otherwise(F.lit("")))
            .filter('source_system is not null')
            .filter('counterparty_code is not null')
    )

    logger.info(f'Filtering all_dates with NO source_system after count: {all_dates_df.count()}')

    return all_dates_df

# COMMAND ----------

def construct_all_dates(spark_session: SparkSession) -> DataFrame:
    all_dates_df = clean_all_dates(spark_session)

    # do we have more than one raw client name for a given shortcode/system
    # combination? if so it points to restatements not being sent to us after a
    # raw client name change
    non_restated_df = (
        all_dates_df
            .groupBy(F.col('source_system'), F.col('counterparty_code'), F.concat('source_system', 'counterparty_code').alias('key'))
            .agg(F.count_distinct('raw_client_name').alias('name_count'))
            .filter(F.col('name_count') > 1)
    )

    window = (
        Window
            .partitionBy('source_system', 'counterparty_code')
            .orderBy('trade_date')
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    if non_restated_df.count() == 0:
        logger.info('No duplicate all_dates found, returning...')

        return (
            all_dates_df
                .withColumn('trade_date', F.last(F.col('trade_date')).over(window))
                .groupBy('source_system', 'counterparty_code', 'raw_client_name')
                .agg(F.last('client_value_cad').alias('cv_cad'), F.last('client_value_usd').alias('cv_usd'), F.last('trade_date').alias('trade_date'))
        )

    logger.info('Handling duplicates (likely caused because trades were not restated by upstream source)')

    # get all client trades NOT restated
    all_dates_last_counterparty_df = (
        all_dates_df
            .withColumn('key', F.concat('source_system', 'counterparty_code'))
            .join(non_restated_df.select('key'), on='key')
            .withColumn('raw_client_name', F.last(F.col('raw_client_name')).over(window))
            .withColumn('trade_date', F.last(F.col('trade_date')).over(window))
            .select('source_system', 'counterparty_code', 'raw_client_name', 'client_value_cad', 'client_value_usd', 'trade_date', 'key')
            .distinct()
    )

    all_dates_df = (
        all_dates_df
            .withColumn('key', F.concat('source_system', 'counterparty_code'))
            .withColumn('trade_date', F.last(F.col('trade_date')).over(window))
    )

    logger.info(f'Clearing non-restated from all_dates... before count: {all_dates_df.count()}')

    all_dates_df = (
        all_dates_df
            .join(all_dates_last_counterparty_df.select('key'), on="key", how="leftanti")
            .select('source_system', 'counterparty_code', 'raw_client_name', 'client_value_cad', 'client_value_usd', 'trade_date')
    )
    all_dates_last_counterparty_df = all_dates_last_counterparty_df.drop('key')

    logger.info(f'Clearing non-restated from all_dates... after count: {all_dates_df.count()}')
    logger.info('Adding latest counterparty code name (based on latest trade date) back to all_dates')

    all_dates_df = all_dates_df.unionByName(all_dates_last_counterparty_df)

    all_dates_df = (
        all_dates_df
            .groupBy('source_system', 'counterparty_code', 'raw_client_name')
            .agg(F.last('client_value_cad').alias('cv_cad'), F.last('client_value_usd').alias('cv_usd'), F.last('trade_date').alias('trade_date'))
    )
    all_dates_df = all_dates_df.dropDuplicates(['source_system', 'counterparty_code'])

    logger.info(f'Total grouped count (also after dropping duplicates): {all_dates_df.count()}')

    return all_dates_df

# COMMAND ----------

def remove_deleted_accounts(httpx_client: Client,
                            spark_session: SparkSession,
                            source_df: DataFrame,
                            join_clause: List[str],
                            filter_clause: str = 'match_type = "POTENTIAL"') -> None:
    assert 'POTENTIAL' in filter_clause

    logger.info('Looking for accounts no longer in the system')

    client_df = spark_session.table(CLIENT_CODE_MATCHES_TABLE)
    client_df = (
        client_df.filter(filter_clause)
    )

    missing_matches = (
        client_df.alias('cf')
            .join(source_df.alias('sf'),
                  on=join_clause,
                  how='leftanti')
    )

    if missing_matches.isEmpty():
        logger.info('All potential matches have existing data, nothing further to do')

        return

    # FIX: snapshot the ids once. The lazy `missing_matches` was re-evaluated by the MERGE after
    # the server delete, so Delta could delete a different set of rows than the server did.
    missing_ids = [r.id for r in missing_matches.select('id').distinct().collect()]

    logger.info(f'Deleting {len(missing_ids)} potential matches not in the data set')

    ids_to_delete = [i.upper() for i in missing_ids]

    logger.info(f'Removing from webserver....')

    delete_server_by_id(httpx_client, ids_to_delete,
                        f'{CLIENT_MATCHING_WEB_SERVER_BASE_URL}/client-code-matches-bulk/bulk-delete')

    logger.info(f'Removing existing POTENTIAL matches from delta table now...')

    missing_ids_df = spark_session.createDataFrame([(i,) for i in missing_ids], 'id string')

    client_dt = DeltaTable.forName(spark_session, CLIENT_CODE_MATCHES_TABLE)
    client_dt.alias('om')\
        .merge(missing_ids_df.alias('cm'), 'om.id = cm.id')\
        .whenMatchedDelete().execute()

    logger.info(f'Removed non-existent potential data matches from delta table')

# COMMAND ----------

# End of Notebook