# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC **Note: **This is a tactical solution to load the data for US Prime sent in emails which will be changed once the strategic solution is implemented
# MAGIC

# COMMAND ----------

# DBTITLE 1,Initialize dependencies
%run "./adls_util.py"

# COMMAND ----------

# DBTITLE 1,Import Error Util
%run "./error_utils.py"

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.types import *
from pyspark.sql import functions as F
import logging

# COMMAND ----------

# DBTITLE 1,Set catalog context
dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

SILVER_US_PRIME_TABLE = f"`{CATALOG_NAME}`.`silver`.`us_prime`"

# COMMAND ----------

# DBTITLE 1,create silver table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_US_PRIME_TABLE} (
        trade_date DATE,
        counterparty_code STRING,
        raw_client_name STRING,
        product_level_0 STRING,
        product_level_1 STRING,
        product_level_2 STRING,
        client_value_usd DECIMAL(25,18),
        source_system STRING,
        file_name_path STRING,
        brnz_ingestion_timestamp TIMESTAMP,
        slvr_ingestion_timestamp TIMESTAMP
    )
""")