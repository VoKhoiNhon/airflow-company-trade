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
    "retries": 0,
}

# Define the DAG
with DAG(
    dag_id="bangladesh_pipeline",
    default_args=default_args,
    description="Bangladesh: Raw->Bronze->Silver->Gold",
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "bangladesh"],
) as dag:
    # Load Variables then formulate the payload
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    bangladesh_payload = ast.literal_eval(Variable.get("bangladesh_payload"))
    use_spark_cluster = bangladesh_payload.get("use_spark_cluster", "ssh_spark_00")
    ssh_spark_cluster = Variable.get(use_spark_cluster)
    str_payload = json.dumps(bangladesh_payload).replace('"', '\\"')

    # Task to run the Python script via SSH
    with TaskGroup("Bronze_Layer") as bronze:
        bronze_bangladesh_company = SSHOperator(
            task_id="bronze_bangladesh_company",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/bronze/bangladesh/bangladesh_company.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )
        bronze_bangladesh_trade_import = SSHOperator(
            task_id="bronze_bangladesh_trade_import",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/bronze/bangladesh/bangladesh_trade_import.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )
        bronze_bangladesh_trade_export = SSHOperator(
            task_id="bronze_bangladesh_trade_export",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/bronze/bangladesh/bangladesh_trade_export.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )

        # DAGs Graph
        bronze_bangladesh_company
        bronze_bangladesh_trade_export
        bronze_bangladesh_trade_import

    with TaskGroup("Silver_Layer") as silver:
        h_company_silver_bangladesh = SSHOperator(
            task_id="h_company_silver_bangladesh",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/h_company/h_company__src_bangladesh_company.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )

        s_company_address_silver_bangladesh = SSHOperator(
            task_id="s_company_address_silver_bangladesh",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_address/s_company_address__src_bangladesh_company.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )

        s_company_demographic_silver_bangladesh = SSHOperator(
            task_id="s_company_demographic_silver_bangladesh",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_demographic/s_company_demographic__src_bangladesh_company.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )
        s_bol_silver_bangladesh_import = SSHOperator(
            task_id="s_bol_silver_bangladesh_import",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/s_bol/s_bol__src_bangladesh_import.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )
        s_bol_silver_bangladesh_export = SSHOperator(
            task_id="s_bol_silver_bangladesh_export",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/s_bol/s_bol__src_bangladesh_export.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )

        l_bol_silver_bangladesh_import = SSHOperator(
            task_id="l_bol_silver_bangladesh_import",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/l_bol/l_bol__src_bangladesh_import.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )
        l_bol_silver_bangladesh_export = SSHOperator(
            task_id="l_bol_silver_bangladesh_export",
            ssh_conn_id=use_spark_cluster,
            command=f"{entrypoint_cmd} \"python3 jobs/silver/l_bol/l_bol__src_bangladesh_export.py --payload '{str_payload}'\"",
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )

        # DAGs Graph
        h_company_silver_bangladesh
        s_bol_silver_bangladesh_import
        s_bol_silver_bangladesh_export
        (h_company_silver_bangladesh >> s_company_address_silver_bangladesh)
        (h_company_silver_bangladesh >> s_company_demographic_silver_bangladesh)
        (
            [
                h_company_silver_bangladesh,
                s_bol_silver_bangladesh_export,
            ]
            >> l_bol_silver_bangladesh_export
        )
        (
            [
                h_company_silver_bangladesh,
                s_bol_silver_bangladesh_import,
            ]
            >> l_bol_silver_bangladesh_import
        )

    # Define task dependencies if more tasks are added
    bronze >> silver
