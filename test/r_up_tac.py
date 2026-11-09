# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC **Note: **This is a tactical solution to load the data for US Prime sent in emails which will be changed once the strategic solution is implemented

# COMMAND ----------

import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
# volume_util.py reads CATALOG_NAME via os.getenv, so the widget value must
# be pushed into the environment BEFORE it runs.
os.environ["CATALOG_NAME"] = dbutils.widgets.get("CATALOG")

# COMMAND ----------

# DBTITLE 1,Initialize dependencies
%run "./volume_util.py"

# COMMAND ----------

# DBTITLE 1,Import libraries
# Dependent Libraries
from datetime import datetime, date, timedelta
from delta.tables import DeltaTable
from io import StringIO
import io
import pyarrow
import logging
from pyarrow import flight
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Setup Environment
dbutils.widgets.text("environment", "")
dbutils.widgets.text("dremio_username", "")
dbutils.widgets.text("dremio_personal_access_token", "")
dbutils.widgets.text("from_date", "")
dbutils.widgets.text("to_date", "")

# COMMAND ----------

# DBTITLE 1,Setup Credentials
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

# COMMAND ----------

env = dbutils.widgets.get("environment").lower()
dremio_username = dbutils.widgets.get('dremio_username')
dremio_personal_access_token = dbutils.widgets.get('dremio_personal_access_token')
from_date = dbutils.widgets.get('from_date')
to_date = dbutils.widgets.get('to_date')

# COMMAND ----------

# DBTITLE 1,Authenticate Dremio
hostname = {
    'dev': 'valrsinfo-atgk1.dev.azure.td.com',
    'pat': 'infoplatform-dremio-pat.corp.tdsecurities.com',
    'prod': 'infoplatform-dremio.corp.tdsecurities.com',
}

# COMMAND ----------

port = 32010
client = flight.FlightClient('grpc+tcp://' + hostname[env] + ':' + str(port))
bearer_token = client.authenticate_basic_token(dremio_username, dremio_personal_access_token)
options = flight.FlightCallOptions(headers=[bearer_token])

# COMMAND ----------

# DBTITLE 1,Function to upload files to Volume
def process_pnl_account(from_date: str, to_date: str = None):
    try:
        # If to_date is not provided, set it to from_date
        if not to_date:
            to_date = from_date

        # Parse the from date and to_date
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")

        # Loop through all dates in the range
        current_date = from_date_obj
        while current_date <= to_date_obj:
            process_date = current_date.strftime("%Y%m%d")
            year = process_date[:4]
            month = process_date[4:6]
            day = process_date[6:8]

            # Define the components for the equities query
            equities_prefix = "pb_client_pnl"
            account_prefix = "client_name_account_mapping"
            extension = ".parquet"

            # Construct the filenames
            equities_filename = f"{equities_prefix}_{process_date}{extension}"
            account_filename = f"{account_prefix}_{process_date}{extension}"

            filenames = [
                equities_filename,
                account_filename
            ]

            for filename in filenames:
                try:
                    query = f'SELECT * FROM "INFO-HDFS"."deepgreen"."user"."tan8193"."data"."us_prime".{filename}'

                    try:
                        info = client.get_flight_info(
                            flight.FlightDescriptor.for_command(query),
                            options
                        )
                        reader = client.do_get(info.endpoints[0].ticket, options)
                        table = reader.read_all()
                        dremio_data = table.to_pandas()
                    except Exception as query_error:
                        print(f"File does not exist for {process_date}: {filename}. Skipping to the next file.")
                        continue  # Skip to the next file

                    target_dir = f"{VOLUME_BASE_PATH}/trades/us_prime/{year}/{month}/{day}"
                    dbutils.fs.mkdirs(target_dir)
                    target_path = f"{target_dir}/{filename}"

                    parquet_buffer = io.BytesIO()
                    dremio_data.to_parquet(parquet_buffer, index=False)
                    parquet_bytes = parquet_buffer.getvalue()

                    with open(target_path, "wb") as f:
                        f.write(parquet_bytes)

                    print(f"Processed and uploaded file: {filename}")
                except Exception as e:
                    print(f"Error during processing file {filename}: {e}")
                    continue  # Skip to the next file

            # Move to the next date
            current_date += timedelta(days=1)

    except Exception as e:
        raise Exception(f"Error during processing: {e}")

# COMMAND ----------

# DBTITLE 1,Call the function
process_pnl_account(from_date, to_date)