import json
import ast
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="silver2gold_company",
    default_args=default_args,
    description="DAG to run silver2gold company",
    schedule_interval=None,  # Set to None for manual runs
    catchup=False,  # Set to False to avoid backfilling
    tags=["silver2gold", "company"],
) as dag:
    # Load Variables then formulate the payload
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    silver2gold_company_payload = ast.literal_eval(
        Variable.get("silver2gold_company_payload")
    )
    use_spark_cluster = silver2gold_company_payload.get(
        "use_spark_cluster", "ssh_spark_00"
    )
    ssh_spark_cluster = Variable.get(use_spark_cluster)
    str_payload = (
        json.dumps(silver2gold_company_payload)
        .replace('"', '\\"')
        .replace("'", "'\\''")
    )

    with TaskGroup("verify_company_name") as verify:
        s_company_regex_name = SSHOperator(
            task_id="s_company_regex_name",
            ssh_conn_id=ssh_spark_cluster,  # The connection ID configured in Airflow
            command=f"{entrypoint_cmd} \"python3 jobs/silver/l_company_verification/s_company_regex_name.py --payload '{str_payload}'\"",
            do_xcom_push=True,  # Set to True if you want to capture command output
            cmd_timeout=None,
            conn_timeout=None,
        )

        verify_by_regex_name = SSHOperator(
            task_id="verify_by_regex_name",
            ssh_conn_id=ssh_spark_cluster,  # The connection ID configured in Airflow
            command=f'{entrypoint_cmd} "python3 jobs/silver/l_company_verification/verify_by_regex_name.py"',
            do_xcom_push=True,  # Set to True if you want to capture command output
            cmd_timeout=None,
            conn_timeout=None,
        )

        verify_by_levenshtein = SSHOperator(
            task_id="verify_by_levenshtein",
            ssh_conn_id=ssh_spark_cluster,  # The connection ID configured in Airflow
            command=f"{entrypoint_cmd} \"python3 jobs/silver/l_company_verification/verify_by_levenshtein.py --payload '{str_payload}'\"",
            do_xcom_push=True,  # Set to True if you want to capture command output
            cmd_timeout=None,
            conn_timeout=None,
        )

        merge_all_verify = SSHOperator(
            task_id="merge_all_verify",
            ssh_conn_id=ssh_spark_cluster,  # The connection ID configured in Airflow
            command=f'{entrypoint_cmd} "python3 jobs/silver/l_company_verification/merge_all_verify.py"',
            do_xcom_push=True,  # Set to True if you want to capture command output
            cmd_timeout=None,
            conn_timeout=None,
        )

        (
            s_company_regex_name
            >> verify_by_regex_name
            >> verify_by_levenshtein
            >> merge_all_verify
        )

    with TaskGroup("business_vault") as business_vault:
        bridge_company_verified_reg = SSHOperator(
            task_id="bridge_company_verified_reg",
            ssh_conn_id=ssh_spark_cluster,  # The connection ID configured in Airflow
            # command=f"{entrypoint_cmd} \"python3 jobs/silver/business_vault/bridge_company_verified_reg.py --payload '{str_payload}'\"",
            command=f"{entrypoint_cmd} \"python3 jobs/silver/business_vault/bridge_company_verified_reg.py --payload '{str_payload}'\"",
            do_xcom_push=True,  # Set to True if you want to capture command output
            cmd_timeout=None,
            conn_timeout=None,
        )

        bridge_company_demographic = SSHOperator(
            task_id="bridge_company_demographic",
            ssh_conn_id=ssh_spark_cluster,  # The connection ID configured in Airflow
            # command=f"{entrypoint_cmd} \"python3 jobs/silver/business_vault/bridge_company_demographic.py --payload '{str_payload}'\"",
            command=f"{entrypoint_cmd} \"python3 jobs/silver/business_vault/bridge_company_demographic_continute.py --payload '{str_payload}'\"",
            do_xcom_push=True,  # Set to True if you want to capture command output
            cmd_timeout=None,
            conn_timeout=None,
        )
        bridge_company_verified_reg >> bridge_company_demographic

    # with TaskGroup("gold") as gold:
    #     company_service = SSHOperator(
    #         task_id="entity_company",
    #         ssh_conn_id=ssh_conn_id,  # The connection ID configured in Airflow
    #         command=f"{entrypoint_cmd} \"python3 jobs/gold/company_service/entity_company.py --payload '{str_payload}'\"",
    #         do_xcom_push=True,  # Set to True if you want to capture command output
    #         cmd_timeout=None,
    #         conn_timeout=None,
    #     )

    verify >> business_vault
