# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching Engine - ADF Trigger Poller
# MAGIC Reads schedules and in-flight runs for the Matching Engine ADF triggers and publishes the next-run state to the Matching Engine timer service.
# MAGIC No table or storage access in this notebook, so no catalog context is required.
# MAGIC

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./json_utils.py"

# COMMAND ----------

# MAGIC %run "./access_token_util.py"

# COMMAND ----------

# MAGIC %run "./adf_api_utils.py"

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("azure_resource_group", "")
dbutils.widgets.text("azure_factory_name", "")
dbutils.widgets.text("azure_version", "2018-06-01")
dbutils.widgets.text("azure_url", "https://management.azure.com/")
dbutils.widgets.text("tenant_id", "")
dbutils.widgets.text("subscription_id", "")
dbutils.widgets.text("adb_client_id_key", "")
dbutils.widgets.text("adb_client_secret_key", "")
dbutils.widgets.text("client_id", "")
dbutils.widgets.text("client_secret", "")
dbutils.widgets.text(
    "matching_engine_triggers",
    "CIP_TRUE_MATCH_NIGHT_TRIGGER,CIP_TRUE_MATCH_WEEKEND_TRIGGER,CIP_TRUE_MATCH_FREQ_TRIGGER",
)
dbutils.widgets.text("matching_engine_url", "")
dbutils.widgets.text("scope", "")

RESOURCE_GROUP = dbutils.widgets.get("azure_resource_group")
FACTORY_NAME = dbutils.widgets.get("azure_factory_name")
API_VERSION = dbutils.widgets.get("azure_version")
BASE_URL = dbutils.widgets.get("azure_url")
TENANT_ID = dbutils.widgets.get("tenant_id")
SUBSCRIPTION_ID = dbutils.widgets.get("subscription_id")
ADB_CLIENT_ID_KEY = dbutils.widgets.get("adb_client_id_key")
ADB_CLIENT_SECRET_KEY = dbutils.widgets.get("adb_client_secret_key")
CLIENT_ID = dbutils.widgets.get("client_id")
CLIENT_SECRET = dbutils.widgets.get("client_secret")
RELEVANT_TRIGGERS = [
    t.strip() for t in dbutils.widgets.get("matching_engine_triggers").split(",") if t.strip()
]
SERVER_URL = dbutils.widgets.get("matching_engine_url").rstrip("/")
SCOPE = dbutils.widgets.get("scope")

# COMMAND ----------

# DBTITLE 1,Collect ADF trigger schedules and running jobs
import json
from datetime import datetime

from httpx import Client

MAX_FUTURE_RUNS = 5

FACTORY_PATH = (
    f"/subscriptions/{SUBSCRIPTION_ID}"
    f"/resourceGroups/{RESOURCE_GROUP}"
    f"/providers/Microsoft.DataFactory/factories/{FACTORY_NAME}"
)

trigger_schedules = []
current_running_jobs = []
times = []

with capture_errors("matching_engine_adf_trigger_poll"), Client(verify=False) as adf_http_client:
    adf_params = ADFParams(
        scope=SCOPE,
        client_id_key=ADB_CLIENT_ID_KEY,
        client_secret_key=ADB_CLIENT_SECRET_KEY,
        tenant_id=TENANT_ID,
        base_url=BASE_URL,
        factory_path=FACTORY_PATH,
        api_version=API_VERSION,
        trigger_name="",
        client=adf_http_client,
    )

    for trigger in RELEVANT_TRIGGERS:
        adf_params.trigger_name = trigger

        # Only next run + (MAX_FUTURE_RUNS) upcoming runs per trigger can ever reach the final top-N.
        trigger_sched = get_trigger_schedule(adf_params, next_runs=MAX_FUTURE_RUNS)

        # Non-ScheduleTrigger types return no next_run / upcoming_runs keys.
        if not trigger_sched.get("next_run"):
            print(f"Skipping trigger {trigger}: {trigger_sched.get('note', 'no computable schedule')}")
            continue

        trigger_sched.update(get_trigger_state(adf_params))
        trigger_schedules.append(trigger_sched)

        times.append(datetime.fromisoformat(trigger_sched["next_run"]))
        times.extend(datetime.fromisoformat(d) for d in trigger_sched["upcoming_runs"])

        current_running_jobs.extend(get_running_runs_for_trigger(adf_params, trigger))

if not trigger_schedules:
    raise ValueError(
        f"No schedulable triggers found among {RELEVANT_TRIGGERS}; nothing to publish."
    )

# De-duplicate runs that fire at the same instant on more than one trigger.
times = sorted(set(times))[:MAX_FUTURE_RUNS]

min_schedule = min(trigger_schedules, key=lambda x: x["next_run_as_timestamp"])

schedule_result = ScheduleResult(
    trigger_state=min_schedule["runtime_state"].lower(),
    iana_time_zone=min_schedule["iana_time_zone"],
    current_running_jobs=current_running_jobs,
    future_runs=times,
)

state = json.loads(serialize(schedule_result))
display(state)

# COMMAND ----------

# DBTITLE 1,Publish state to the Matching Engine timer service
def _extract_timers(response_json: dict) -> list:
    # An empty HAL collection may omit "_embedded" entirely.
    return (response_json.get("_embedded") or {}).get("matchingEngineTimers", [])


with capture_errors("matching_engine_timer_publish"), Client(verify=False) as me_client:
    token = get_oauth2_access_token(CLIENT_ID, CLIENT_SECRET)
    headers = {"Authorization": f"Bearer {token}"}
    timer_url = f"{SERVER_URL}/matching-engine-timer"
    payload = {"state": state}

    response = me_client.get(timer_url, headers=headers)
    response.raise_for_status()
    timers = _extract_timers(response.json())

    # Keep a single timer record: remove any duplicates beyond the first.
    for timer in timers[1:]:
        me_client.delete(f"{timer_url}/{timer['id']}", headers=headers).raise_for_status()

    if not timers:
        me_client.post(timer_url, json=payload, headers=headers).raise_for_status()
        print("Created matching engine timer record")
    else:
        me_client.put(
            f"{timer_url}/{timers[0]['id']}", json=payload, headers=headers
        ).raise_for_status()
        print(f"Updated matching engine timer record {timers[0]['id']}")