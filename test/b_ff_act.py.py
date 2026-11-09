# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import re
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("year_month", "")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

year_month = dbutils.widgets.get("year_month")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paths / table names (Unity Catalog)
# MAGIC
# MAGIC <p>
# MAGIC REMOVED vs hive_metastore version: get_adls_base_path(), bronze_ff_act_path (it was defined but never actually used to set a table LOCATION - the original CREATE TABLE was already an implicit-location managed table, so this converts cleanly to a UC-managed table with no LOCATION change needed). 
# MAGIC
# MAGIC Two raw-path variables are kept, matching the pattern used in </br>
# MAGIC
# MAGIC bronze_td_cowen_tactical_uc.py:
# MAGIC         - raw_ff_act_volume_path: where we actually read the raw parquet from (the Volume mount, /Volumes/...) </br>
# MAGIC
# MAGIC         - raw_ff_act_external_path: the underlying ADLS storage location, used only for the file_name_path lineage column and the replaceWhere predicate, so that value stays stable/meaningful regardless of the UC mount path.
# MAGIC </p>

# COMMAND ----------

raw_ff_act_volume_path = f"{VOLUME_BASE_PATH}/trades/fund_financing_act"
raw_ff_act_external_path = f"{EXTERNAL_LOCATION_BASE_PATH}/trades/fund_financing_act"

bronze_table = f"`{CATALOG_NAME}`.`bronze`.`fund_financing_act`"

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

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_table} (
    `Client` STRING,
    `Interest_Income` DECIMAL(18, 2),
    `Cost_Of_Funds_Expense` DECIMAL(18, 2),
    `Fee_Ongoing_Net_Interest` DECIMAL(18, 2),
    `Amort_Interest_Income` DECIMAL(18, 2),
    `Ops_Treasury_Breakfunding_Exp` DECIMAL(18, 2),
    `Treasury_Breakfunding_Income` DECIMAL(18, 2),
    `Syndication_Fee_MNGR` DECIMAL(18, 2),
    `Fee_Loans_Direct_Net_Interest` DECIMAL(18, 2),
    `Grand_Total` DECIMAL(18, 2),
    `Allocation` DECIMAL(18, 2),
    `FTP` DECIMAL(18, 2),
    `EoE` DOUBLE,
    `PCL` DECIMAL(18, 2),
    `NIBT` DECIMAL(18, 2),
    `NIAT` DECIMAL(18, 2),
    `file_name_path` STRING,
    `ingestion_timestamp` TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# Strip invalid Spark/Delta column name characters: space , ; { } ( ) \n \t =
def clean_column_names(df):
    for col in df.columns:
        clean = re.sub(r"[ ,;{}()\n\t=]", "", col)  # remove invalid chars
        clean = clean.replace(" ", "_")               # replace spaces with _
        clean = re.sub(r"_+", "_", clean).strip("_") # collapse multiple underscores
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df

# COMMAND ----------

def align_schema_to_bronze(df: DataFrame, bronze_table: str) -> DataFrame:
    """
    Cast columns in raw_df to match the data types of the existing bronze Delta table.
    - Columns in df that don't exist in the bronze table are left as-is.
    - Columns in the bronze table that are missing from df are ignored.
    - Only type mismatches are cast; matching columns are untouched.
    """
    bronze_schema = {
        field.name: field.dataType
        for field in spark.table(bronze_table).schema.fields
    }

    aligned_cols = []
    for field in df.schema.fields:
        bronze_type = bronze_schema.get(field.name)
        if bronze_type is not None and field.dataType != bronze_type:
            aligned_cols.append(
                F.col(field.name).cast(bronze_type).alias(field.name)
            )
        else:
            aligned_cols.append(F.col(field.name))

    return df.select(aligned_cols)

# COMMAND ----------

with capture_errors("bronze_fund_financing_act_ingestion"):
    # Get Latest File
    raw_file_path = f"{raw_ff_act_volume_path}/{year}/{month:02d}"

    # Get all files in directory, find the one with the greatest name
    files = dbutils.fs.ls(raw_file_path)

    # Filter only parquet files and sort by name descending
    parquet_files = sorted(
        [f for f in files if f.name.endswith(".parquet")],
        key=lambda f: f.name,
        reverse=True
    )

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {raw_file_path}")

    latest_file = parquet_files[0].path

# COMMAND ----------

 # Lineage path: swap the Volume mount prefix for the underlying external
# storage location, so file_name_path stays consistent whether read via the
# /Volumes/... mount or the raw ADLS location.
latest_file_external = latest_file.replace(
    f"dbfs:{VOLUME_BASE_PATH}", EXTERNAL_LOCATION_BASE_PATH
    ).replace(
        VOLUME_BASE_PATH, EXTERNAL_LOCATION_BASE_PATH
    )

# COMMAND ----------

raw_df = (
        spark.read
        .option("ignoreCorruptFiles", "false")
        .option("ignoreMissingFiles", "false")
        .parquet(f"{latest_file}")
    )

# COMMAND ----------

raw_df = clean_column_names(raw_df)
raw_df = raw_df.withColumn("ingestion_timestamp", F.current_timestamp())
raw_df = raw_df.withColumn("file_name_path", F.lit(f"{latest_file_external}"))

# COMMAND ----------

# Align raw df to bronze schema
# We ignore any additional columns
bronze_columns = spark.table(bronze_table).columns
raw_df = raw_df.select(bronze_columns)

# COMMAND ----------

raw_df = align_schema_to_bronze(raw_df, bronze_table)

# COMMAND ----------

(
        raw_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"file_name_path LIKE '{raw_ff_act_external_path}/{year}/{month:02d}/%'"
        )
        .saveAsTable(bronze_table)
    )