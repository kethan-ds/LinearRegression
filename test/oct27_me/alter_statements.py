# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching Engine - Schema Alterations and Watermark Reset (Unity Catalog)
# MAGIC Idempotent: safe to re-run. Adds new columns to Matching Engine raw tables and resets
# MAGIC the `raw.prism_last_modified` watermark to epoch so the next run reprocesses everything.
# MAGIC

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# DBTITLE 1,Catalog context
# Child notebooks do not inherit catalog context - CATALOG must be passed in (ADF / controller).
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()

if not CATALOG_NAME:
    raise ValueError("CATALOG widget must be set (e.g. d4001-centralus-tdsci-cipcatalog).")

spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# DBTITLE 1,Helpers
def _assert_table_exists(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Table `{CATALOG_NAME}`.{table_name} does not exist in Unity Catalog. "
            f"Create/migrate it before running this notebook."
        )


def add_column(table_name: str, column_name: str, column_type: str) -> None:
    """Add a column only if it is not already present (schema check, not error-text matching)."""
    _assert_table_exists(table_name)

    existing_columns = {c.lower() for c in spark.table(table_name).columns}
    if column_name.lower() in existing_columns:
        print(f"Column {column_name} already exists in {table_name}, skipping")
        return

    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (`{column_name}` {column_type})")
    print(f"Added column {column_name} {column_type} to {table_name}")

# COMMAND ----------

# DBTITLE 1,Add new columns
with capture_errors("matching_engine_add_columns"):
    add_column("raw.truematch_account_diffs", "did_acct_id_change", "BOOLEAN")
    add_column("raw.prism_data", "merged_with", "STRING")
    add_column(
        "raw.prism_data",
        "merged_accounts",
        "ARRAY<STRUCT<merged_account_id: STRING, merged_time: STRING>>",
    )

# COMMAND ----------

# DBTITLE 1,Reset prism_last_modified watermark to epoch
# HMS version located the table's storage path and overwrote it as raw parquet.
# Under UC, path-based access to managed table storage is blocked, so the reset is done
# as a governed UPDATE on the table itself. Row count is unchanged, as before.
with capture_errors("matching_engine_reset_prism_last_modified"):
    _assert_table_exists("raw.prism_last_modified")

    spark.sql("""
        UPDATE raw.prism_last_modified
        SET last_modified = TIMESTAMP '1970-01-01 00:00:00'
    """)

# COMMAND ----------

# DBTITLE 1,Validate
display(spark.sql("""
    SELECT count(1) AS row_count,
           min(last_modified) AS min_last_modified,
           max(last_modified) AS max_last_modified
    FROM raw.prism_last_modified
"""))