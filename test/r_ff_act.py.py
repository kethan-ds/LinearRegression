# Databricks notebook source
from pyarrow import flight
from pyarrow import parquet as pq
from datetime import datetime
import logging
import sys
import os

# COMMAND ----------

dbutils.widgets.text("environment", "")
dbutils.widgets.text("dremio_username", "")
dbutils.widgets.text("dremio_personal_access_token", "")
dbutils.widgets.text("year_month", "")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

env = dbutils.widgets.get("environment").lower()
dremio_username = dbutils.widgets.get("dremio_username")
dremio_personal_access_token = dbutils.widgets.get("dremio_personal_access_token")
year_month = dbutils.widgets.get("year_month")

# COMMAND ----------

logger = logging.getLogger("raw_fund_financing_act")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))
    logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

if len(year_month) != 6 or not year_month.isdigit():
    raise ValueError("Value must be a 6-digit string in YYYYMM format")

try:
    parsed = datetime.strptime(year_month, "%Y%m")
    year = parsed.year
    month = parsed.month
except ValueError:
    raise ValueError("Invalid year_month value")

# COMMAND ----------

hostname = {
    "dev": "valrsinfo-atgk1.dev.azure.td.com",
    "pat": "infoplatform-dremio-pat.corp.tdsecurities.com",
    "prod": "infoplatform-dremio.corp.tdsecurities.com",
}

port = 32010
client = flight.FlightClient(
    "grpc+tcp://" + hostname[env] + ":" + str(port)
)

# COMMAND ----------

bearer_token = client.authenticate_basic_token(
    dremio_username,
    dremio_personal_access_token,
)

# COMMAND ----------

options = flight.FlightCallOptions(headers=[bearer_token])

# COMMAND ----------

with capture_errors("raw_fund_financing_act_ingestion"):
    sql = f"""
    SELECT *
    FROM "INFO-HDFS"."deepgreen"."user"."tan8193"."data"."fund_financing_act"
    WHERE dir0 = {year}
      AND dir1 = {month}
      AND dir2 = (
          SELECT MAX(dir2)
          FROM "INFO-HDFS"."deepgreen"."user"."tan8193"."data"."fund_financing_act"
          WHERE dir0 = {year}
            AND dir1 = {month}
      )
    """

    info = client.get_flight_info(
        flight.FlightDescriptor.for_command(sql),
        options,
    )
    reader = client.do_get(info.endpoints[0].ticket, options)

    table = reader.read_all()
    upload_date = table["dir2"][0].as_py()
    table = table.drop(["dir0", "dir1", "dir2"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write directly to the Unity Catalog Volume
# MAGIC <p>
# MAGIC REMOVED vs hive_metastore version: get_directory_client(...).get_file_client(...) + file_client.upload_data(). UC Volumes are FUSE-mounted at /Volumes/..., so pyarrow can write the parquet file straight to the mount path - no ADLS SDK, no BytesIO buffer, no storage credential needed in the notebook.
# MAGIC </p>
# MAGIC

# COMMAND ----------

target_dir = f"{VOLUME_BASE_PATH}/trades/fund_financing_act/{year}/{month:02d}"
    dbutils.fs.mkdirs(target_dir)

    output_file_name = f"m_fund_financing_act_{upload_date}.parquet"
    output_path = f"{target_dir}/{output_file_name}"

    if os.path.exists(output_path):
        logger.info(
            f"{output_file_name} already exists in the Volume for {year}-{month:02d}. "
            f"It will be overwritten."
        )

    pq.write_table(table, output_path)

    logger.info(f"Successfully written file: {output_path}")