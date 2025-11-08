# ======================================================
# DAG Name: test_config_dag
# Purpose:  Verify Airflow + Config setup
# Author:   Rohan
# ======================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import yaml
import os

# ======================================================
# Load Configuration
# ======================================================
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "../config/config.yaml")
    with open(config_path, "r") as f:
        cfg_d = yaml.safe_load(f)
    return cfg_d

# ======================================================
# Task Function
# ======================================================
def test_config_print(**context):
    cfg = load_config()

    env = cfg["ENVIRONMENT"]["DEV"]
    paths = cfg["PATHS"]

    print("✅ Airflow DAG is running correctly!")
    print("📂 Environment:", env)
    print("📁 Raw Zone Path:", paths["RAW_ZONE"])
    print("📁 Staging Zone Path:", paths["STAGING_ZONE"])
    print("📁 Curated Zone Path:", paths["CURATED_ZONE"])
    print("🪵 Logs Path:", paths["LOGS_PATH"])

# ======================================================
# Default Args
# ======================================================
default_args = {
    "owner": "Rohan",
    "start_date": datetime(2025, 11, 8),
    "retries": 0,
}

# ======================================================
# DAG Definition
# ======================================================
with DAG(
    dag_id="SMFG_DW_test_config_dag",
    default_args=default_args,
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["SMFG_Datawarehouse"],
) as dag:

    test_run = PythonOperator(
        task_id="print_config_test",
        python_callable=test_config_print,
        provide_context=True,
    )

    test_run
