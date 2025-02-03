import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="data4seo_bronze_pipeline",
    default_args=default_args,
    description="DAG to run data4seo.py using BashOperator",
    schedule_interval="@yearly",  # Adjust the schedule as needed
    start_date=datetime(2024, 12, 1),  # Replace with your desired start date
    catchup=False,  # Set to False to avoid backfilling
    tags=["bronze", "data4seo"],
) as dag:
    # Load Variables then formulate the payload
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    str_payload = json.dumps(Variable.get("data4seo_payload"))[1:-1]
    ssh_conn_id = Variable.get("ssh_spark_00")

    # Task to run the Python script via SSH
    raw_to_bronze_data4seo = SSHOperator(
        task_id="raw_to_bronze_data4seo",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/bronze/data4seo/data4seo.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    hub_company_bronze_to_silver_data4seo = SSHOperator(
        task_id="hub_company_bronze_to_silver_data4seo",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/h_company/data4seo/h_company_data4seo.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_company_address_bronze_to_silver_data4seo = SSHOperator(
        task_id="sat_company_address_bronze_to_silver_data4seo",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_address/s_company_address__src_data4seo.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_company_demographic_bronze_to_silver_data4seo = SSHOperator(
        task_id="sat_company_demographic_bronze_to_silver_data4seo",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_demographic/s_company_demographic__src_data4seo.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    # Define task dependencies if more tasks are added
    raw_to_bronze_data4seo >> hub_company_bronze_to_silver_data4seo
    (
        hub_company_bronze_to_silver_data4seo
        >> sat_company_address_bronze_to_silver_data4seo
    )
    (
        hub_company_bronze_to_silver_data4seo
        >> sat_company_demographic_bronze_to_silver_data4seo
    )
