import json
import ast
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="kentucky_pipeline",
    default_args=default_args,
    description="DAG to run kentucky.py using BashOperator",
    schedule_interval="@daily",  # Adjust the schedule as needed
    start_date=datetime(2025, 1, 23),  # Replace with your desired start date
    catchup=False,  # Set to False to avoid backfilling
    tags=["raw", "kentucky"],
) as dag:
    # Load Variables then formulate the payload
    today_date = datetime.now().strftime("%Y%m%d")
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    crawler_entrypoint_cmd = Variable.get("crawler_entrypoint_cmd")
    ssh_crawler = Variable.get("ssh_crawler")

    with TaskGroup("Raw_Layer") as raw:
        crawl_to_raw_kentucky = SSHOperator(
            task_id="crawl_to_raw_kentucky",
            ssh_conn_id=ssh_crawler,
            command=f'{crawler_entrypoint_cmd} "python3 jobs/kentucky/kentucky.py"',
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )
    
    raw
