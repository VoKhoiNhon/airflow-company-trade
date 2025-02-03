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
    dag_id="newfoundland_and_labrador_bronze_pipeline",
    default_args=default_args,
    description="DAG to run newfoundland_and_labrador.py using BashOperator",
    schedule_interval="@yearly",  # Adjust the schedule as needed
    start_date=datetime(2024, 12, 1),  # Replace with your desired start date
    catchup=False,  # Set to False to avoid backfilling
    tags=["bronze", "newfoundland_and_labrador"],
) as dag:
    # Load Variables then formulate the payload
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    str_payload = json.dumps(Variable.get("newfoundland_and_labrador_payload"))[
        1:-1
    ]  # Removes the outer double quotes
    ssh_conn_id = Variable.get("ssh_spark_00")

    # Task to run the Python script via SSH
    raw_to_bronze_newfoundland_and_labrador = SSHOperator(
        task_id="raw_to_bronze_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/bronze/newfoundland_and_labrador/newfoundland_and_labrador_company_person.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    hub_company_bronze_to_silver_newfoundland_and_labrador = SSHOperator(
        task_id="hub_company_bronze_to_silver_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/silver/h_company/h_company__src_newfoundland_and_labrador.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_company_address_bronze_to_silver_newfoundland_and_labrador = SSHOperator(
        task_id="sat_company_address_bronze_to_silver_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_address/s_company_address__src_newfoundland_and_labrador_company_address.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_company_demographic_bronze_to_silver_newfoundland_and_labrador = SSHOperator(
        task_id="sat_company_demographic_bronze_to_silver_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_demographic/s_company_demographic__src_newfoundland_and_labrador.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    hub_person_bronze_to_silver_newfoundland_and_labrador = SSHOperator(
        task_id="hub_person_bronze_to_silver_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/silver/h_person/h_person__src_newfoundland_and_labrador.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_person_demographic_bronze_to_silver_newfoundland_and_labrador = SSHOperator(
        task_id="sat_person_demographic_bronze_to_silver_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_person_demographic/s_person_demographic__src_newfoundland_and_labrador.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    link_person_to_company_newfoundland_and_labrador = SSHOperator(
        task_id="link_person_to_company_newfoundland_and_labrador",
        ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
        command=f"{entrypoint_cmd} \"python3 jobs/silver/l_person_company_relationship/l_person_company_relationship__src_newfoundland_and_labrador.py --payload '{str_payload}'\"",
        do_xcom_push=True,  # Set to True if you want to capture command output
        cmd_timeout=None,
        conn_timeout=None,
    )

    # Define task dependencies if more tasks are added
    (
        raw_to_bronze_newfoundland_and_labrador
        >> hub_company_bronze_to_silver_newfoundland_and_labrador
    )
    (
        hub_company_bronze_to_silver_newfoundland_and_labrador
        >> sat_company_address_bronze_to_silver_newfoundland_and_labrador
    )
    (
        hub_company_bronze_to_silver_newfoundland_and_labrador
        >> sat_company_demographic_bronze_to_silver_newfoundland_and_labrador
    )

    (
        raw_to_bronze_newfoundland_and_labrador
        >> hub_person_bronze_to_silver_newfoundland_and_labrador
    )
    (
        hub_person_bronze_to_silver_newfoundland_and_labrador
        >> sat_person_demographic_bronze_to_silver_newfoundland_and_labrador
    )
    (
        hub_person_bronze_to_silver_newfoundland_and_labrador
        >> link_person_to_company_newfoundland_and_labrador
    )
