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
    dag_id="nbd_trade_data_pipeline",
    default_args=default_args,
    description="DAG to run nbd_trade_data using BashOperator",
    schedule_interval=None,  # Adjust the schedule as needed
    start_date=datetime(2024, 12, 1),  # Replace with your desired start date
    catchup=False,  # Set to False to avoid backfilling
    tags=["bronze", "nbd_trade_data"],
) as dag:
    # Load Variables then formulate the payload
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    str_payload = json.dumps(Variable.get("nbd_trade_data_payload"))[1:-1]
    ssh_conn_id = Variable.get("ssh_spark_00")

    # Task to run the Python script via SSH
    # raw_to_bronze__nbd_trade_data = SSHOperator(
    #     task_id="raw_to_bronze_nbd_trade_data",
    #     ssh_conn_id=ssh_conn_id,
    #     command=f"{entrypoint_cmd} python3 jobs/bronze/trade_nbd_data/trade_nbd_data.py --payload '{str_payload}'",
    #     do_xcom_push=True,
    #     cmd_timeout=None,
    #     conn_timeout=None,
    # )

    link_bol__nbd_trade_data = SSHOperator(
        task_id="link_bol__nbd_trade_data",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/l_bol/l_bol__src_trade_nbd.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_bol__nbd_trade_data = SSHOperator(
        task_id="sat_bol__nbd_trade_data",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_bol/s_bol__src_trade_nbd.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    hub_company__nbd_trade_data = SSHOperator(
        task_id="hub_company__nbd_trade_data",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/h_company/h_company__src_trade_nbd.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    sat_company_demographic__nbd_trade_data = SSHOperator(
        task_id="sat_company__nbd_trade_data",
        ssh_conn_id=ssh_conn_id,
        command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_demographic/s_company_demographic__src_trade_nbd.py --payload '{str_payload}'\"",
        do_xcom_push=True,
        cmd_timeout=None,
        conn_timeout=None,
    )

    (
        # raw_to_bronze__nbd_trade_data
        link_bol__nbd_trade_data
        >> sat_bol__nbd_trade_data
        >> hub_company__nbd_trade_data
        >> sat_company_demographic__nbd_trade_data
    )
