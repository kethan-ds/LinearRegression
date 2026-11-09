# Databricks notebook source
import functools
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import re
import calendar
from datetime import datetime, date
from pyspark.sql import Window


# COMMAND ----------

dbutils.widgets.text("year_month", "")

# COMMAND ----------

year_month = dbutils.widgets.get("year_month")

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

# MAGIC %md
# MAGIC ### Table names (Unity Catalog)
# MAGIC <p>
# MAGIC REMOVED vs hive_metastore version: get_adls_base_path() and the raw_ff_act_path variable it built - that variable was defined but never actually referenced anywhere else in this notebook (silver only ever reads from the bronze table, never the raw path directly), so it's dropped rather than converted.
# MAGIC </p>

# COMMAND ----------

bronze_table = f"`{CATALOG_NAME}`.`bronze`.`fund_financing_act`"
silver_table = f"`{CATALOG_NAME}`.`silver`.`fund_financing_act`"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table} (
    `source_system` STRING,
    `trade_date` DATE,
    `product_level_0` STRING,
    `product_level_1` STRING,
    `product_level_2` STRING,
    `raw_client_name` STRING,
    `counterparty_code` STRING,
    `security_description` STRING,
    `is_client_facing` STRING,
    `client_value_usd` DECIMAL(18, 7),
    `fiscal_year` INT,

    `client` STRING,
    `interest_income` DECIMAL(18, 2),
    `cost_of_funds_expense` DECIMAL(18, 2),
    `fee_ongoing_net_interest` DECIMAL(18, 2),
    `amort_interest_income` DECIMAL(18, 2),
    `ops_treasury_breakfunding_exp` DECIMAL(18, 2),
    `treasury_breakfunding_income` DECIMAL(18, 2),
    `syndication_fee_mngr` DECIMAL(18, 2),
    `fee_loans_direct_net_interest` DECIMAL(18, 2),
    `grand_total` DECIMAL(18, 2),
    `allocation` DECIMAL(18, 2),
    `ftp` DECIMAL(18, 2),
    `eoe` DOUBLE,
    `pcl` DECIMAL(18, 2),
    `nibt` DECIMAL(18, 2),
    `niat` DECIMAL(18, 2),
    `file_name_path` STRING,
    `ingestion_timestamp` TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

def add_months(d: date, n: int) -> date:
    """1st-of-month date `n` months from `d` (handles year rollover)."""
    i = d.year * 12 + (d.month - 1) + n
    return date(i // 12, i % 12 + 1, 1)


def fiscal_year_bounds(d: date):
    """
    Return (fiscal_year, fy_start, fy_end) for period `d`.

    The fiscal year starts in November, so Nov/Dec roll into the next
    calendar year's fiscal year.

    fy_start / fy_end are trade-date markers (the first day of the FY's
    first and last months): Nov-1 of (fy-1) and Oct-1 of fy.
    """
    fy = d.year + 1 if d.month >= 11 else d.year
    return fy, date(fy - 1, 11, 1), date(fy, 10, 1)

# COMMAND ----------

# Column expression: parse the trade date (1st of the month) out of file_name_path.
# The path looks like ".../{year}/{month:02d}/file.parquet", so we grab the first
# "/YYYY/MM/" segment. This is unaffected by the UC migration - bronze now stores
# file_name_path using the external storage location rather than the /Volumes/...
# mount path, but the /YYYY/MM/ segment structure is preserved either way.

# COMMAND ----------

TRADE_DATE = F.make_date(
    F.regexp_extract("file_name_path", r"/(\d{4})/(\d{2})/", 1).cast("int"),
    F.regexp_extract("file_name_path", r"/(\d{4})/(\d{2})/", 2).cast("int"),
    F.lit(1),
)

# COMMAND ----------

# ============================================================================
# RESOLVE RUN MODE + READ / WRITE RANGES
# ============================================================================

out_start = date(year, month, 1)
fy, fy_start, fy_end = fiscal_year_bounds(out_start)

# Is this a restatement?
# It is a restatement if this year/month is already in silver.
silver_exists = spark.catalog.tableExists(silver_table)

is_restatement = (
    silver_exists
    and spark.read.table(silver_table)
        .filter(F.col("file_name_path").like(f"%/{year}/{month:02d}/%"))
        .limit(1)
        .count() > 0
)

# Are there already-loaded months after this one in the same fiscal year?
latest_fy_month = (
    spark.read.table(silver_table)
        .filter(
            (F.col("fiscal_year") == fy)
            & (F.col("trade_date") > F.lit(out_start))
        )
        .agg(F.max("trade_date").alias("m"))
        .collect()[0]["m"]
    if silver_exists
    else None
)

has_later_months = latest_fy_month is not None

# WRITE range [out_start .. out_end]
out_end = latest_fy_month if has_later_months else out_start

# READ range [read_start .. read_end]
# Read the entire FY so lag can reach the most recent previous month.
read_start = fy_start
read_end = out_end

print(
    f"FY{fy} | restatement={is_restatement} | later_months={has_later_months} "
    f"| write {out_start}..{out_end} | read {read_start}..{read_end}"
)

# COMMAND ----------

bronze_act = (
    spark.read.table(bronze_table)
    .withColumn("trade_date", TRADE_DATE)
    .filter(F.col("trade_date").between(read_start, read_end))
)

# COMMAND ----------

# Fail loudly if the target month has no bronze rows.
if bronze_act.filter(F.col("trade_date") == out_start).limit(1).count() == 0:
    raise ValueError(
        f"No bronze data found for {out_start.isoformat()} in {bronze_table}. "
        f"Refusing to write an empty result for the target month "
        f"(this would delete existing rows without replacing them)."
    )

# COMMAND ----------

bronze_act.display()

# COMMAND ----------

# lag over the FY-wide window returns the NIBT of the most recent previous
# month that exists for the client. When there is no prior month in the FY,
# lag is NULL and coalesce(0) makes client_value = NIBT.
w = Window.partitionBy("Client").orderBy("trade_date")
prev_nibt = F.coalesce(
    F.lag("NIBT").over(w),
    F.lit(0).cast("decimal(18,2)")
)

silver_act = (
    bronze_act
    .withColumn(
        "client_value_usd",
        (F.col("NIBT") - prev_nibt).cast("decimal(18,7)")
    )
    .filter(F.col("trade_date").between(out_start, out_end))
    .withColumn("fiscal_year", F.lit(fy))
)

# COMMAND ----------

silver_act = (
    silver_act
    .withColumn("source_system", F.lit("FUND_FINANCE"))
    .withColumn("is_client_facing", F.lit(1))
    .withColumn("product_level_0", F.lit("Fund Finance"))
    .withColumn("product_level_1", F.lit("Fund Finance"))
    .withColumn("product_level_2", F.lit("Fund Finance"))
    .withColumn("raw_client_name", F.col("Client"))
    .withColumn("counterparty_code", F.col("Client"))
    .withColumn("security_description", F.col("Client"))
)

# COMMAND ----------

def snake_case_convert(name: str) -> str:
    # 1. Strip surrounding whitespace
    name = name.strip()
    # 2. Replace special chars with a space (word boundary)
    name = re.sub(r"[#()/\\:;,.]", " ", name)
    # 3. Collapse multiple spaces
    name = re.sub(r"\s+", " ", name).strip()
    # 4. Spaces -> underscores
    name = name.replace(" ", "_")
    # 5. Lowercase
    name = name.lower()
    # 6. Collapse consecutive underscores
    name = re.sub(r"_+", "_", name)
    # 7. Strip leading/trailing underscores
    name = name.strip("_")
    return name


for col in silver_act.columns:
    new_col = snake_case_convert(col)
    if new_col != col:
        silver_act = silver_act.withColumnRenamed(col, new_col)

# COMMAND ----------

def align_schema_to_silver(df: DataFrame, silver_table: str) -> DataFrame:
    silver_fields = spark.table(silver_table).schema.fields

    missing = [f.name for f in silver_fields if f.name not in df.columns]
    if missing:
        raise ValueError(
            f"DataFrame is missing required silver columns: {missing}"
        )

    aligned_cols = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in silver_fields
    ]
    return df.select(aligned_cols)

# COMMAND ----------

# NOTE (UC migration / bug fix): the hive_metastore version called this with a
# hardcoded "silver.fund_financing_act" string instead of the silver_table
# variable defined above - harmless there since they resolved to the same
# unqualified name, but under UC an unqualified name would resolve against
# whatever catalog/schema happens to be current, not necessarily CATALOG_NAME.
# Using the silver_table variable makes this unambiguous.
silver_act = align_schema_to_silver(
    silver_act,
    silver_table
)

# COMMAND ----------

replace_where = (
    f"fiscal_year = {fy} "
    f"AND trade_date >= '{out_start.isoformat()}' "
    f"AND trade_date <= '{out_end.isoformat()}'"
)

(
    silver_act.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", replace_where)
    .saveAsTable(silver_table)
)