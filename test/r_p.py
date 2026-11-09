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

# MAGIC %run "./access_token_util.py"

# COMMAND ----------

from datetime import date
from datetime import datetime
from dateutil.parser import parse

import os
import json
import requests
import datetime
import pandas as pd
from pyspark.sql.functions import col
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

spark.conf.set('spark.sql.session.timeZone', 'UTC')

PANORAMIC_TRACKER_TABLE = f"`{CATALOG_NAME}`.`raw`.`panoramic_tracker`"
PANORAMIC_TRADES_BASE_PATH = f"{VOLUME_BASE_PATH}/trades/panoramic"

# COMMAND ----------

dbutils.widgets.text("TRADE_DATE", "")
dbutils.widgets.text("MODIFIED_DATE", "")
dbutils.widgets.text("EPOCH_START_DATE", "2022-11-01")
dbutils.widgets.text("rerun_dates", "[]")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("ADB_AUTH_CLIENT_ID", "")
dbutils.widgets.text("ADB_AUTH_CLIENT_SECRET", "")
# Giving the option of overriding auth url since dev uses pat auths
dbutils.widgets.text("OVERRIDE_AUTH_URL", "")
dbutils.widgets.text("TDSCI_RIVENDELL_AUTH_URL", "")

# COMMAND ----------

rerun_dates = eval(dbutils.widgets.get("rerun_dates"))
environment = dbutils.widgets.get('environment').lower()
override_auth_url = dbutils.widgets.get('OVERRIDE_AUTH_URL')
TDSCI_RIVENDELL_AUTH_URL = dbutils.widgets.get("TDSCI_RIVENDELL_AUTH_URL")

# For client id and secret we have to pass them in either way, so we'll trust that the auths being passed in will be correct for the given env
ADB_AUTH_CLIENT_ID = dbutils.widgets.get('ADB_AUTH_CLIENT_ID')
ADB_AUTH_CLIENT_SECRET = dbutils.widgets.get('ADB_AUTH_CLIENT_SECRET')

input_trade_date = dbutils.widgets.get('TRADE_DATE')
PANORAMIC_BASE_URL = os.getenv('GFI_PANORAMIC_URL')
MODIFIED_DATE = dbutils.widgets.get("MODIFIED_DATE")
EPOCH_START_DATE = dbutils.widgets.get("EPOCH_START_DATE")

# COMMAND ----------

# if no date provided, default to previous weekday
trade_date = None
if (not input_trade_date or input_trade_date.isspace()) and datetime.date.today().weekday() != 0:
    trade_date = datetime.date.today() - datetime.timedelta(days=1)
elif (not input_trade_date or input_trade_date.isspace()):
    trade_date = datetime.date.today() - datetime.timedelta(days=3)
else:
    trade_date = parse(input_trade_date).date()

logger.info(f"trade_date = {trade_date}")

# COMMAND ----------

# If no modified date provided, set to one weekday before the trade date to catch unusually timed updates and previous day failures
modified_date = None
if MODIFIED_DATE:
    modified_date = parse(MODIFIED_DATE).date()
elif trade_date.weekday() == 0:  # trade date is Monday, so previous weekday is Friday
    modified_date = trade_date - datetime.timedelta(days=3)
else:
    modified_date = trade_date - datetime.timedelta(days=1)

logger.info(f"modified_date = {modified_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get list of Trade Dates with Updated Data Since Modified Date
# MAGIC

# COMMAND ----------

PANORAMIC_READINESS_ENDPOINT = PANORAMIC_BASE_URL + '.markets.getClient360TradesReadiness'

def get_panoramic_trade_readiness_data(start_trade_date: str=None, end_trade_date: str=None, modified_date: str=None) -> dict:
    """
    This function returns a json of date-by-date data on whether data in Panoramic has been modified since the modified date, and whether that data is ready
    for processing.
    """
    # Get Auth
    access_token = get_oauth2_access_token(ADB_AUTH_CLIENT_ID, ADB_AUTH_CLIENT_SECRET, TDSCI_RIVENDELL_AUTH_URL)
    headers = {}
    headers["Authorization"] = f"Bearer {access_token}"
    params = f'business=FI&business=FX&business=Equities&startTradeDate={start_trade_date}&endTradeDate={end_trade_date}&modifiedDate={modified_date}'

    response = requests.request(
        method="GET",
        url=PANORAMIC_READINESS_ENDPOINT,
        params=params,
        data={},
        headers=headers,
        verify=False,
        stream=True
    )
    response.raise_for_status()
    return response.json()

# COMMAND ----------


if rerun_dates: # Rerun list of Dates given,
    process_date_list = rerun_dates
    logger.info(f"Processing list of Dates provided in rerun_dates\n{rerun_dates}")
else:
    if modified_date == datetime.date(2022, 11, 1): # Weekly Full Rerun
        #Generate a list of Weekdays (YYYY-MM-DD String) from 2022-11-01 to trade_date
        days = (modified_date + datetime.timedelta(days=i)
                for i in range((trade_date - modified_date).days + 1)
               )
        process_date_list = [d.strftime("%Y-%m-%d") for d in days if d.weekday() < 5]
        logger.info(f"Processing {len(process_date_list)} Weekdays\nStarting from {modified_date} to {trade_date}")
    else: # Daily run
        # Only Run 1 day (trade_date) which would be the previous weekday
        process_date_list = [trade_date.strftime("%Y-%m-%d")]
        logger.info(f"Processing 1 day: {trade_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get GFI Pano Trade Data for relevant dates

# COMMAND ----------

PANORAMIC_TRADES_URL = PANORAMIC_BASE_URL + '.markets.getNormalizedClientValue'

def get_panoramic_trade_data(start_trade_date: str=None, end_trade_date: str=None, trade_date: str=None) -> dict:
    # Get auths
    access_token = get_oauth2_access_token(ADB_AUTH_CLIENT_ID, ADB_AUTH_CLIENT_SECRET, TDSCI_RIVENDELL_AUTH_URL)
    headers = {}
    headers["Authorization"] = f"Bearer {access_token}"
    params = f'business=FI&business=FX&business=Equities&startTradeDate={trade_date}&endTradeDate={trade_date}'

    response = requests.request(
        method="GET",
        url=PANORAMIC_TRADES_URL,
        params=params,
        data={},
        headers=headers,
        verify=False,
        stream=True
    )
    response.raise_for_status()
    return response

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Data to the Volume

# COMMAND ----------


def save_panoramic_trade_file(trade_date: str):
    """
    This function retrieves data from Panoramic for a given date and saves the raw file to a designated directory in the UC Volume
    """
    year = trade_date[0:4]
    month = trade_date[5:7]

    directory_path = f"{PANORAMIC_TRADES_BASE_PATH}/{year}/{month}"
    file_path = f"{directory_path}/panoramic_trades_{trade_date}.json"

    data_response = get_panoramic_trade_data(trade_date=trade_date)

    dbutils.fs.mkdirs(directory_path)
    with open(file_path, "wb") as file:
        file.write(data_response.content)

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Up Updated Date Tracker

# COMMAND ----------

# Using a table that tracks whether a date's file has been updated during this raw data retrieval
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PANORAMIC_TRACKER_TABLE} (
        trade_date STRING,
        file_is_updated BOOLEAN
    ) USING DELTA
""")

# COMMAND ----------

for trade_date in process_date_list:
    logger.info(f"Processing {trade_date}\n")
    save_panoramic_trade_file(trade_date)

# Logging to track what days were actually updated on a given run.
updated_trade_dates_for_log = sorted([str(trade_date) for trade_date in process_date_list])
logger.info("Completed Panoramic trade file refresh. Updated trade dates this run: %s", updated_trade_dates_for_log)

# COMMAND ----------


from pyspark.sql.types import StringType, BooleanType, StructField, StructType
from delta.tables import DeltaTable

# If this ran successfully, we can mark all of the dates that have been updated as having been updated in the tracking table
tracking_schema = StructType([
    StructField("trade_date", StringType(), False),
    StructField("file_is_updated", BooleanType(), False)
])

tracking_df = spark.createDataFrame([(processed_date, True) for processed_date in process_date_list], tracking_schema)

# Load the existing Tracking Delta Table
tracking_table = DeltaTable.forName(spark, PANORAMIC_TRACKER_TABLE)

# Merge in any dates updated as True
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

# COMMAND ----------

# log that updated trade date files are updated for bronze ingestion
logger.info("Marked trade dates as Updated in panoramic_tracker: %s", updated_trade_dates_for_log)
