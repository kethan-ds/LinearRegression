# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("CATALOG", "")
CATALOG_NAME = dbutils.widgets.get("CATALOG").strip()
if not CATALOG_NAME:
    raise ValueError("CATALOG parameter is required")

dbutils.widgets.text("TRADE_DATE", "2025-09-09")
dbutils.widgets.text("MODIFIED_DATE", "2025-09-09")
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

# Panoramic DEV uses PAT Rivendell auths, so if environment is dev we use pat auths for authentication
if environment == 'dev' and override_auth_url:
    TDSCI_RIVENDELL_AUTH_URL = override_auth_url
else:
    TDSCI_RIVENDELL_AUTH_URL = dbutils.widgets.get("TDSCI_RIVENDELL_AUTH_URL")

# For client id and secret we have to pass them in either way, so we'll trust that the auths being passed in will be correct for the given env
ADB_AUTH_CLIENT_ID = dbutils.widgets.get('ADB_AUTH_CLIENT_ID')
ADB_AUTH_CLIENT_SECRET = dbutils.widgets.get('ADB_AUTH_CLIENT_SECRET')

input_trade_date = dbutils.widgets.get('TRADE_DATE')
PANORAMIC_BASE_URL = os.getenv('GFI_PANORAMIC_URL')
MODIFIED_DATE = dbutils.widgets.get("MODIFIED_DATE")
EPOCH_START_DATE = dbutils.widgets.get("EPOCH_START_DATE")

# COMMAND ----------

try:
    dbutils.notebook.run("./raw_panoramic.py", 18000, {
        "CATALOG": CATALOG_NAME,
        "TRADE_DATE": input_trade_date,
        "MODIFIED_DATE": MODIFIED_DATE,
        "EPOCH_START_DATE": EPOCH_START_DATE,
        "rerun_dates": str(rerun_dates),
        "environment": environment,
        "ADB_AUTH_CLIENT_ID": ADB_AUTH_CLIENT_ID,
        "ADB_AUTH_CLIENT_SECRET": ADB_AUTH_CLIENT_SECRET,
        "OVERRIDE_AUTH_URL": override_auth_url,
        "TDSCI_RIVENDELL_AUTH_URL": TDSCI_RIVENDELL_AUTH_URL
    })
except Exception as e:
    raise Exception(f"Error occurred while running raw notebook: {e}")

# COMMAND ----------

try:
    dbutils.notebook.run("./bronze_panoramic.py", 600, {"CATALOG": CATALOG_NAME})
except Exception as e:
    raise Exception(f"Error occurred while running bronze notebook: {e}")

# COMMAND ----------

try:
    dbutils.notebook.run("./silver_panoramic.py", 600, {"CATALOG": CATALOG_NAME})
except Exception as e:
    raise Exception(f"Error occurred while running silver notebook: {e}")
