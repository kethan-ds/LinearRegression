# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC **Note: **This is a tactical solution to load the data for US Prime sent in emails which will be changed once the strategic solution is implemented

# COMMAND ----------

import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")

# COMMAND ----------

# volume_util.py reads CATALOG_NAME via os.getenv, so the widget value must
# be pushed into the environment BEFORE it runs.
os.environ["CATALOG_NAME"] = dbutils.widgets.get("CATALOG")

# COMMAND ----------

# DBTITLE 1,Initialize dependencies
%run "./volume_util.py"

# COMMAND ----------

# DBTITLE 1,Import Libraries
from datetime import datetime, date, timedelta
from pyspark.dbutils import DBUtils
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name, regexp_replace

import re
import requests
import logging
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Set Configurations
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

dbutils.widgets.text('ADLS_DESTINATION_PATH', '')
ADLS_DESTINATION_PATH = dbutils.widgets.get('ADLS_DESTINATION_PATH')
dbutils.widgets.text('from_date', '')
from_date = dbutils.widgets.get('from_date')
dbutils.widgets.text('to_date', '')
to_date = dbutils.widgets.get('to_date')

BRONZE_PB_CLIENT_PNL_TABLE = f"`{CATALOG_NAME}`.`bronze`.`us_prime_pb_client_pnl`"
BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE = f"`{CATALOG_NAME}`.`bronze`.`us_prime_client_name_account_mapping`"


# COMMAND ----------

# Volume (FUSE) path -- strip a legacy "data/raw/" prefix if the caller
# still passes one, and root everything under the Volume.
_destination_relative_path = ADLS_DESTINATION_PATH.strip("/")
for _legacy_prefix in ("data/raw/", "data/raw"):
    if _destination_relative_path.startswith(_legacy_prefix):
        _destination_relative_path = _destination_relative_path[len(_legacy_prefix):].lstrip("/")
        break

# COMMAND ----------

raw_data_folder = f"{VOLUME_BASE_PATH}/{_destination_relative_path}"

# COMMAND ----------

# DBTITLE 1,Create bronze tables if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PB_CLIENT_PNL_TABLE} (
        Client STRING,
        ManagerShortName STRING,
        Calendar_Day_PnL DECIMAL(25,18),
        Effective STRING,
        Sales STRING,
        source_system STRING,
        file_name_path STRING,
        ingestion_timestamp TIMESTAMP
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE} (
        Account STRING,
        Fund_Name STRING,
        Client STRING,
        Effective STRING,
        source_system STRING,
        file_name_path STRING,
        ingestion_timestamp TIMESTAMP
    )
""")

# COMMAND ----------

# Check if the column exist in the table
columns_pnl_df = spark.sql(f"DESCRIBE TABLE {BRONZE_PB_CLIENT_PNL_TABLE}")
columns_pnl = [row.col_name for row in columns_pnl_df.collect()]

# COMMAND ----------

# Add the column if they do not exist
if "file_received_timestamp" not in columns_pnl:
    spark.sql(f"ALTER TABLE {BRONZE_PB_CLIENT_PNL_TABLE} ADD COLUMNS (file_received_timestamp TIMESTAMP)")


# COMMAND ----------

# Check if the column exist in the table
columns_acc_df = spark.sql(f"DESCRIBE TABLE {BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE}")
columns_acc = [row.col_name for row in columns_acc_df.collect()]


# COMMAND ----------

# Add the column if they do not exist
if "file_received_timestamp" not in columns_acc:
    spark.sql(f"ALTER TABLE {BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE} ADD COLUMNS (file_received_timestamp TIMESTAMP)")

# COMMAND ----------

# DBTITLE 1,Define logger
# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    handlers=[
        logging.StreamHandler()  # Output logs to the console
    ]
)

# COMMAND ----------

# Create a logger instance
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Load data from Volume into bronze

def get_files_to_process(dir_path, from_date, to_date=None):
    # If to_date is not provided, set it to from_date
    if not to_date:
        to_date = from_date

    # Parse the from date and to date
    from_date_obj = datetime.strptime(from_date, "%Y%m%d")
    to_date_obj = datetime.strptime(to_date, "%Y%m%d")

    matching_files = []

    # Loop through all dates in the range
    current_date = from_date_obj
    while current_date <= to_date_obj:
        process_date = current_date.strftime("%Y%m%d")
        year = process_date[:4]
        month = process_date[4:6]
        day = process_date[6:8]

        files_path = f"{dir_path}/{year}/{month}/{day}"
        try:
            files = dbutils.fs.ls(files_path)
            files = [f for f in files if not f.isDir()]

            for f in files:
                filename = f.name
                match = re.search(r'\d{8}', filename)
                if match and match.group(0) == process_date:
                    matching_files.append(f)
        except Exception as e:
            print(f"No files found for date {process_date}. Skipping to the next date.")

        # Move to the next date
        current_date += timedelta(days=1)

    return matching_files

# COMMAND ----------

try:
    from_date = datetime.strptime(from_date, "%Y-%m-%d").strftime("%Y%m%d")
    to_date = datetime.strptime(to_date, "%Y-%m-%d").strftime("%Y%m%d")
    files = get_files_to_process(raw_data_folder, from_date, to_date)

    for file in files:
        path = file.path
        file_name = file.name
        file_received_timestamp = file.modificationTime  # Get the file's received timestamp

        df = spark.read.parquet(path)
        df = df.selectExpr(*[f"CAST(`{col}` AS STRING) AS `{col}`" for col in df.columns])

        file_received_timestamp = datetime.fromtimestamp(file_received_timestamp / 1000.0)
        # Convert file_received_timestamp from milliseconds to a proper timestamp
        df = df.withColumn(
                "file_name_path",
                regexp_replace(
                    input_file_name(),
                    f"dbfs:{VOLUME_BASE_PATH}",
                    EXTERNAL_LOCATION_BASE_PATH,
                ),
            ) \
               .withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_system", lit("US_PRIME")) \
               .withColumn("file_received_timestamp", lit(file_received_timestamp))  # Add the file's received timestamp

        for col_name in df.columns:
            new_col_name = col_name.replace(' ', '_')
            df = df.withColumnRenamed(col_name, new_col_name)

        process_date_match = re.search(r'\d{8}', file_name)
        if process_date_match:
            process_date = process_date_match.group()

            if "pb_client_pnl" in file_name:
                # Check if data for the process date exists in the table
                existing_data = spark.sql(f"""
                    SELECT file_received_timestamp
                    FROM {BRONZE_PB_CLIENT_PNL_TABLE}
                    WHERE Effective = '{process_date}'
                """).collect()

                if existing_data:
                    existing_timestamp = existing_data[0]["file_received_timestamp"]
                    if existing_timestamp is None or file_received_timestamp > existing_timestamp:
                        # Delete existing data for the process date
                        spark.sql(f"""
                            DELETE FROM {BRONZE_PB_CLIENT_PNL_TABLE}
                            WHERE Effective = '{process_date}'
                        """)
                    else:
                        print(f"Skipping file {file_name} as it is older than the existing file.")
                        continue

                df = df.withColumn("Calendar_Day_PnL", col("Calendar_Day_PnL").cast("DECIMAL(25,18)"))

                # Write the new data to the table
                df.write.format("delta") \
                    .mode("append") \
                    .saveAsTable(BRONZE_PB_CLIENT_PNL_TABLE)

            elif "client_name_account_mapping" in file_name:
                # Check if data for the process date exists in the table
                existing_data = spark.sql(f"""
                    SELECT file_received_timestamp
                    FROM {BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE}
                    WHERE Effective = '{process_date}'
                """).collect()

                if existing_data:
                    existing_timestamp = existing_data[0]["file_received_timestamp"]
                    if existing_timestamp is None or file_received_timestamp > existing_timestamp:
                        # Delete existing data for the process date
                        spark.sql(f"""
                            DELETE FROM {BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE}
                            WHERE Effective = '{process_date}'
                        """)
                    else:
                        print(f"Skipping file {file_name} as it is older than the existing file.")
                        continue

                # Write the new data to the table
                df.write.format("delta") \
                    .mode("append") \
                    .saveAsTable(BRONZE_CLIENT_NAME_ACCT_MAPPING_TABLE)

except Exception as e:
    raise Exception(f"Error during processing: {e}")

# COMMAND ----------

# DBTITLE 1,Add logger info
logger.info("Starting SQL query execution.")

df = spark.sql(f"""
    select count(1)
    from {BRONZE_PB_CLIENT_PNL_TABLE}
""")
display(df)

logger.info("SQL query execution completed.")