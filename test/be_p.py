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

# MAGIC %run "./volume_util.py"

# COMMAND ----------

PANORAMIC_TRACKER_TABLE_NAME = f"`{CATALOG_NAME}`.`raw`.`panoramic_tracker`"
BRONZE_TABLE_NAME = f"`{CATALOG_NAME}`.`bronze`.`panoramic`"
PANORAMIC_TRADES_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/panoramic"


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import current_timestamp, max, row_number, col, trim
from delta.tables import DeltaTable
import os
from pyspark.sql.window import Window
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

# MAGIC %md
# MAGIC # Create Bronze Table if Not Exists

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} (
  TradeId               STRING NOT NULL,
  TradeDate             DATE NOT NULL,
  SourceSystem          STRING NOT NULL,
  BusinessLine          STRING,
  Desk                  STRING,
  SubDesk               STRING,
  Book                  STRING,
  Side                  STRING,
  Product               STRING,
  SubProduct            STRING,
  Tenor                 STRING,
  AccountMnemonic       STRING,
  Customer              STRING,
  CustomerSegment       STRING,
  Currency              STRING,
  CurrencySubGroup      STRING,
  SecurityKey           STRING,
  SecurityDescription   STRING,
  MaturityDate          STRING,
  SalesPerson           STRING,
  BusinessCity          STRING,
  BusinessCountry       STRING,
  IndustrySector        STRING,
  PrimaryTradeType      STRING,
  VolumeUsd             DECIMAL(18, 6),
  VolumeCad             DECIMAL(18, 6),
  ClientValueUsd        DECIMAL(18, 6),
  ClientValueCad        DECIMAL(18, 6),
  Cost                  DECIMAL(18, 6),
  Dv01                  DECIMAL(18, 6),
  IsE                   BOOLEAN,
  Business              STRING,
  file_name_path        STRING,
  ingestion_timestamp   TIMESTAMP
)
USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Schema

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Raw and Bronze Data Paths

# COMMAND ----------

raw_data_filepath = PANORAMIC_TRADES_BASE_PATH + "/{year}/{month}/panoramic_trades_{trade_date}.json"

# COMMAND ----------

# MAGIC %md
# MAGIC # Get list of updated dates to process

# COMMAND ----------

# Get days that need to be reprocessed from the tracking table
tracking_table_df = spark.table(PANORAMIC_TRACKER_TABLE_NAME)
updated_dates_df = tracking_table_df.filter(tracking_table_df["file_is_updated"] == True)
updated_dates = [row[0] for row in updated_dates_df.select("trade_date").distinct().collect()]

# Full list of filepaths to updated data can be passed into spark.read function to read all simultaneously and concatenate
list_of_filepaths = [raw_data_filepath.format(year=trade_date[:4], month=trade_date[5:7], trade_date=trade_date) for trade_date in updated_dates]

logger.info(f"Processing {len(list_of_filepaths)}")
logger.info(f"List of Files to Process\n{list_of_filepaths}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Files That Have Been Updated

# COMMAND ----------

bronze_panoramic_schema = spark.table(BRONZE_TABLE_NAME).schema

new_bronze_df = spark.read.json(
    path=list_of_filepaths,
    schema=bronze_panoramic_schema,
).withColumn("file_name_path", col("_metadata.file_path")) \
 .withColumn("ingestion_timestamp", current_timestamp()) \
 .filter(
    col("TradeId").isNotNull() & (trim(col("TradeId")) != "") &
    col("TradeDate").isNotNull() & (trim(col("TradeDate")) != "") &
    col("SourceSystem").isNotNull() & (trim(col("SourceSystem")) != "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop Duplicates on Expected Unique Key
# MAGIC We discovered duplicates on the expected unique key (TradeId, TradeSystem) causing later merges to fail, so we include only the duplicate
# MAGIC that was most recently updated (using the LastModified field)

# COMMAND ----------

# Create a window specification to partition the data and order it by ingestion_timestamp to create the most recent version
window_spec = Window.partitionBy("TradeId", "SourceSystem").orderBy(col("ingestion_timestamp").desc())
# Create temp row number column over window spec to take the highest value (which will be the first due to the descending sort)
new_bronze_df = new_bronze_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Bronze

# COMMAND ----------

existing_bronze_table = DeltaTable.forName(spark, BRONZE_TABLE_NAME)

# Must create list of dates being updated so that trades removed from gfi pano can be deleted only for those days being updated
trade_dates = new_bronze_df.select("TradeDate").distinct().collect()
trade_dates_sql = ",".join([f"'{row.TradeDate}'" for row in trade_dates])

# Trades are taken from veritas and so tradeId and tradeSystem are still a valid unique key
if new_bronze_df.isEmpty():
    logger.info("No new bronze records to merge; skipping.")
else:
    existing_bronze_table.alias("bronze").merge(
        new_bronze_df.alias("bronze_new"),
        "bronze.TradeId <=> bronze_new.TradeId AND bronze.SourceSystem <=> bronze_new.SourceSystem"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .whenNotMatchedBySourceDelete(
        f"bronze.TradeDate IN ({trade_dates_sql})"
    ) \
     .execute()

# Set logging to confirm which dates were successfully merged into bronze
updated_dates_for_log = sorted([str(trade_date) for trade_date in updated_dates])
logger.info("Processed and merged trade dates into bronze gfi panoramic trades: %s", updated_dates_for_log)

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Processed Date Tracker Table

# COMMAND ----------

# Now that dates have been successfully merged we can set file_is_updated back to false
tracking_schema = StructType([
    StructField('trade_date', StringType(), False),
    StructField('file_is_updated', BooleanType(), False)
])

# Set values to false so that files don't get reprocessed unless they've been updated
tracking_df = spark.createDataFrame([(processed_date, False) for processed_date in updated_dates], tracking_schema)

# Load the existing Tracking Delta Table
tracking_table = DeltaTable.forName(spark, PANORAMIC_TRACKER_TABLE_NAME)

# Merge in any dates updated as False
tracking_table.alias("target").merge(
    tracking_df.alias("source"),
    "target.trade_date = source.trade_date"
).whenMatchedUpdate(
    set={"file_is_updated": "source.file_is_updated"}
).whenNotMatchedInsert(
    values={
        "trade_date": "source.trade_date",
        "file_is_updated": "source.file_is_updated"
    }
).execute()
