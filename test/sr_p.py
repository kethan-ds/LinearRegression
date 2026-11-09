# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()
if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")
os.environ["CATALOG_NAME"] = CATALOG_NAME
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------


# COMMAND ----------

import os

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import logging

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    handlers=[
        logging.StreamHandler()  # Output logs to the console
    ]
)

logger = logging.getLogger(__name__)

# COMMAND ----------

BRONZE_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`panoramic`"
SILVER_TABLE_NAME = f"`{CATALOG_NAME}`.`silver`.`panoramic`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE_NAME} (
    trade_id                 STRING NOT NULL,
    trade_date               DATE NOT NULL,
    source_system            STRING NOT NULL,
    product_level_0          STRING,
    product_level_1          STRING,
    product_level_2          STRING,
    product_level_3          STRING,
    book                     STRING,
    direction                STRING,
    sub_product              STRING,
    tenor                    STRING,
    counterparty_code        STRING,
    raw_client_name          STRING,
    client_display_name      STRING,
    client_display_name_type STRING,
    is_client_facing         STRING,
    trade_currency           STRING,
    currency_sub_group       STRING,
    security_id              STRING,
    security_description     STRING,
    maturity_date            DATE,
    salesperson              STRING,
    business_city            STRING,
    business_country         STRING,
    industry_sector          STRING,
    primary_trade_type       STRING,
    volume_usd_mm            DECIMAL(38, 10),
    volume_cad_mm            DECIMAL(38, 10),
    client_value_usd         DECIMAL(38, 6),
    client_value_cad         DECIMAL(38, 6),
    cost                     DECIMAL(38, 6),
    dv01_trade_currency      DECIMAL(38, 6),
    c360_trade_execution_type STRING,
    business                 STRING,
    file_name_path           STRING,
    ingestion_timestamp      TIMESTAMP
)
USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read In Bronze Data

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple Column Renames

# COMMAND ----------

rename_columns = {
    'TradeId': 'trade_id',
    'TradeDate': 'trade_date',
    'SourceSystem': 'source_system',
    'BusinessLine': 'product_level_0',
    'Desk': 'product_level_1',
    'SubDesk': 'product_level_2',
    'Product': 'product_level_3',
    'Book': 'book',
    'Side': 'direction',
    'SubProduct': 'sub_product',
    'Tenor': 'tenor',
    'AccountMnemonic': 'counterparty_code',
    'Customer': 'client_display_name',
    'CustomerSegment': 'client_display_name_type',
    'Currency': 'trade_currency',
    'CurrencySubGroup': 'currency_sub_group',
    'SecurityKey': 'security_id',
    'SecurityDescription': 'security_description',
    'MaturityDate': 'maturity_date',
    'SalesPerson': 'salesperson',
    'BusinessCity': 'business_city',
    'BusinessCountry': 'business_country',
    'VolumeUsd': 'volume_usd_mm',
    'VolumeCad': 'volume_cad_mm',
    'ClientValueUsd': 'client_value_usd',
    'ClientValueCad': 'client_value_cad',
    'Cost': 'cost',
    'Dv01': 'dv01_trade_currency',
    'IsE': 'c360_trade_execution_type',
    'Business': 'business',
    'IndustrySector': 'industry_sector',
    'PrimaryTradeType': 'primary_trade_type',
}

# COMMAND ----------

#Standardize field names to snake_case and add additional columns
silver_df = bronze_df.withColumnsRenamed(rename_columns).withColumn('raw_client_name', col('client_display_name'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Derived Columns

# COMMAND ----------

# Divide volume so it's in millions
silver_df = silver_df.withColumn('volume_cad_mm', col('volume_cad_mm') / 1000000) \
                     .withColumn('volume_usd_mm', col('volume_usd_mm') / 1000000)

# COMMAND ----------

# All trades in gfi panoramic exposed on the API are client facing by default, which is a string 1 in all dates
silver_df = silver_df.withColumn('is_client_facing', lit('1'))

# COMMAND ----------

silver_df = silver_df.withColumn('maturity_date', to_date('maturity_date', 'yyyy-MM-dd'))

# COMMAND ----------

silver_df = silver_df.withColumn(
    'c360_trade_execution_type',
    when(col('c360_trade_execution_type') == True, 'Electronic')
    .when(col('c360_trade_execution_type') == False, 'Voice')
    .otherwise(None)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply Silver Schema

# COMMAND ----------

silver_schema = spark.table(SILVER_TABLE_NAME).schema

# Cast and Select fields for Silver
silver_df = silver_df.select(*[
    col(f.name).cast(f.dataType).alias(f.name)
    for f in silver_schema.fields
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload To Silver

# COMMAND ----------

# Get existing silver table
existing_silver_table = DeltaTable.forName(spark, SILVER_TABLE_NAME)

# Update when the trade exists, insert when it doesn't, delete when trade has been removed
existing_silver_table.alias('silver').merge(
    silver_df.alias('new_silver'),
    'silver.trade_id <=> new_silver.trade_id AND silver.source_system <=> new_silver.source_system'
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .whenNotMatchedBySourceDelete() \
 .execute()

logger.info(f'Processed and merged into silver.panoramic')
