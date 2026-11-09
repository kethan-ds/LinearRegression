# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC **Note: **This is a tactical solution to load the data for US Prime sent in emails which will be changed once the strategic solution is implemented

# COMMAND ----------

# DBTITLE 1,Set Configurations
dbutils.widgets.text("CATALOG", "")
dbutils.widgets.text("ADLS_DESTINATION_PATH", "")
dbutils.widgets.text("ADLS_CONTAINER_NAME", "")
dbutils.widgets.text("ADLS_ACCOUNT_URI", "")
dbutils.widgets.text("environment", "")
dbutils.widgets.text("dremio_username", "")
dbutils.widgets.text("dremio_personal_access_token", "")
dbutils.widgets.text("from_date", "")
dbutils.widgets.text("to_date", "")

# COMMAND ----------

# DBTITLE 1,Get Configurations
CATALOG_NAME = dbutils.widgets.get("CATALOG")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

ADLS_DESTINATION_PATH = dbutils.widgets.get("ADLS_DESTINATION_PATH")
ADLS_CONTAINER_NAME = dbutils.widgets.get("ADLS_CONTAINER_NAME")
ADLS_ACCOUNT_URI = dbutils.widgets.get("ADLS_ACCOUNT_URI")
env = dbutils.widgets.get("environment").lower()
dremio_username = dbutils.widgets.get("dremio_username")
dremio_personal_access_token = dbutils.widgets.get("dremio_personal_access_token")
from_date = dbutils.widgets.get("from_date")
to_date = dbutils.widgets.get("to_date")

# COMMAND ----------

# DBTITLE 1,Run Raw Notebook
try:
    dbutils.notebook.run("./raw_us_prime_tactical.py", 1500, {
        "CATALOG": CATALOG_NAME,
        "environment": env,
        "dremio_username": dremio_username,
        "dremio_personal_access_token": dremio_personal_access_token,
        "from_date": from_date,
        "to_date": to_date
    })
except Exception as e:
    raise Exception(f"Error occurred while running raw notebook: {e}")

# COMMAND ----------

# DBTITLE 1,Run Bronze Notebook
try:
    dbutils.notebook.run("./bronze_us_prime_tactical.py", 1500, {
        "CATALOG": CATALOG_NAME,
        "ADLS_DESTINATION_PATH": ADLS_DESTINATION_PATH,
        "from_date": from_date,
        "to_date": to_date
    })
except Exception as e:
    raise Exception(f"Error occurred while running bronze notebook: {e}")

# COMMAND ----------

# DBTITLE 1,Check for holiday
from datetime import datetime, timedelta
from pyspark.sql.functions import lit

# COMMAND ----------

# Convert from_date and to_date to datetime objects
from_date_obj = datetime.strptime(from_date, '%Y-%m-%d')
to_date_obj = datetime.strptime(to_date, '%Y-%m-%d')

# COMMAND ----------

# Generate all dates between from_date and to_date
all_dates = [from_date_obj + timedelta(days=x) for x in range((to_date_obj - from_date_obj).days + 1)]

# Tables to process
tables = [
    f"`{CATALOG_NAME}`.`bronze`.`us_prime_pb_client_pnl`",
    f"`{CATALOG_NAME}`.`bronze`.`us_prime_client_name_account_mapping`",
]

# COMMAND ----------

BRONZE_HOLIDAY_CALENDAR_TABLE = f"`{CATALOG_NAME}`.`bronze`.`holiday_calendar`"

# COMMAND ----------

# Process each date and table
for table_name in tables:
    for check_date in all_dates:
        # Check if the date is a holiday
        holiday_query = f"""
            SELECT date
            FROM {BRONZE_HOLIDAY_CALENDAR_TABLE}
            WHERE code = 'NYC' AND date = '{check_date.strftime('%Y-%m-%d')}'
        """
        holiday_df = spark.sql(holiday_query)
        is_holiday = not holiday_df.rdd.isEmpty()

        if is_holiday:
            # If it's a holiday, find the last available business day
            last_business_day_query = f"""
                SELECT *
                FROM {table_name}
                WHERE Effective < '{check_date.strftime('%Y%m%d')}'
                ORDER BY Effective DESC
                LIMIT 1
            """

            last_business_day_df = spark.sql(last_business_day_query)

            if last_business_day_df.rdd.isEmpty():
                raise Exception(f"Holiday on {check_date.strftime('%Y-%m-%d')} but no business day data available before this date in {table_name}.")

            # Fetch all rows for the last available business day
            last_business_day = last_business_day_df.collect()[0]["Effective"]
            all_last_business_day_data_query = f"""
                SELECT *
                FROM {table_name}
                WHERE Effective = '{last_business_day}'
            """
            all_last_business_day_data = spark.sql(all_last_business_day_data_query)

            # Update the Effective column to check_date, keeping all columns the same
            df_with_effective = all_last_business_day_data.withColumn("Effective", lit(check_date.strftime('%Y%m%d')))

            # Write the entire data to the table
            df_with_effective.write.format("delta") \
                .mode("append") \
                .saveAsTable(table_name)
        else:
            # If it's not a holiday, check if data exists for this date
            data_check_query = f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE Effective = '{check_date.strftime('%Y%m%d')}'
            """
            data_check_df = spark.sql(data_check_query)
            row_count = data_check_df.collect()[0]["count"]

            if row_count == 0:
                raise Exception(f"Date {check_date.strftime('%Y-%m-%d')} is not a holiday but no data available in {table_name}.")


# COMMAND ----------

# DBTITLE 1,Run Silver Notebook
try:
    dbutils.notebook.run("./silver_us_prime_tactical.py", 500, {
        "CATALOG": CATALOG_NAME,
    })
except Exception as e:
    raise Exception(f"Error occurred while running silver notebook: {e}")