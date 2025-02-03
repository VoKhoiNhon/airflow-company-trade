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
    dag_id="sanction_pipeline",
    default_args=default_args,
    description="DAG to run sanction.py using BashOperator",
    schedule_interval="@daily",  # Adjust the schedule as needed
    start_date=datetime(2024, 12, 1),  # Replace with your desired start date
    catchup=False,  # Set to False to avoid backfilling
    tags=["crawl", "bronze", "silver", "sanction"],
) as dag:
    # Load Variables then formulate the payload
    today_date = datetime.now().strftime("%Y%m%d")
    entrypoint_cmd = Variable.get("entrypoint_cmd")
    crawler_entrypoint_cmd = Variable.get("crawler_entrypoint_cmd")
    ssh_crawler = Variable.get("ssh_crawler")

    # Update today_date into payload
    sanction_company_payload = ast.literal_eval(
        Variable.get("sanction_company_payload")
    )
    sanction_company_payload.update({"load_date": today_date})
    use_spark_cluster = sanction_company_payload.get(
        "use_spark_cluster", "ssh_spark_00"
    )
    ssh_spark_cluster = Variable.get(use_spark_cluster)
    str_payload = json.dumps(sanction_company_payload).replace('"', '\\"')

    with TaskGroup("Raw_Layer") as raw:
        crawl_to_raw_sanction = SSHOperator(
            task_id="raw.sanction",
            ssh_conn_id=ssh_crawler,
            command=f'{crawler_entrypoint_cmd} "python3 jobs/sanction/run.py"',
            do_xcom_push=True,
            cmd_timeout=None,
            conn_timeout=None,
        )

    with TaskGroup("Bronze_Layer") as bronze:
        with TaskGroup("Schema_Company") as bronze_sanction_company:
            bronze_sanction_schema_company = SSHOperator(
                task_id="bronze.sanction.company.company",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_company.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            bronze_sanction_schema_organization = SSHOperator(
                task_id="bronze.sanction.company.organization",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_organization.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            bronze_sanction_schema_company >> bronze_sanction_schema_organization
        with TaskGroup("Schema_Address") as bronze_sanction_address:
            bronze_sanction_schema_address = SSHOperator(
                task_id="bronze.sanction.address",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_address.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
        with TaskGroup("Schema_Person") as bronze_sanction_person:
            bronze_sanction_schema_person = SSHOperator(
                task_id="bronze.sanction.person",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_person.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
        with TaskGroup("Schema_Santion") as bronze_sanction_sanction:
            bronze_sanction_schema_sanction = SSHOperator(
                task_id="bronze.sanction.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
        with TaskGroup("Schema_EntityLink") as bronze_sanction_entity_link:
            bronze_sanction_schema_family = SSHOperator(
                task_id="bronze.sanction.entity_link.family",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_family.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            bronze_sanction_schema_membership = SSHOperator(
                task_id="bronze.sanction.entity_link.membership",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_membership.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            bronze_sanction_schema_ownership = SSHOperator(
                task_id="bronze.sanction.entity_link.ownership",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_ownership.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            bronze_sanction_schema_directorship = SSHOperator(
                task_id="bronze.sanction.entity_link.directorship",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_directorship.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            bronze_sanction_schema_employment = SSHOperator(
                task_id="bronze.sanction.entity_link.employment",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_employment.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            bronze_sanction_schema_unknownlink = SSHOperator(
                task_id="bronze.sanction.entity_link.unknownlink",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/bronze/sanction/sanction_unknownlink.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            (
                bronze_sanction_schema_family
                >> bronze_sanction_schema_membership
                >> bronze_sanction_schema_ownership
                >> bronze_sanction_schema_directorship
                >> bronze_sanction_schema_employment
                >> bronze_sanction_schema_unknownlink
            )

    with TaskGroup("Silver_Layer") as silver:
        with TaskGroup("Sanction_Company") as sanction_company:
            silver_h_company_sanction = SSHOperator(
                task_id="silver.h_company.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/h_company/h_company__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_s_company_address = SSHOperator(
                task_id="silver.s_company_address.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_address/s_company_address__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )

            silver_s_company_demographic = SSHOperator(
                task_id="silver.s_company_demographic.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/s_company_demographic/s_company_demographic__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            (silver_h_company_sanction >> silver_s_company_address)
            (silver_h_company_sanction >> silver_s_company_demographic)
        with TaskGroup("Sanction_Person") as sanction_person:
            silver_h_person_sanction = SSHOperator(
                task_id="silver.h_person.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/h_person/h_person__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_s_person_demographic_sanction = SSHOperator(
                task_id="silver.s_person_demographic.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/s_person_demographic/s_person_demographic__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_s_person_address_sanction = SSHOperator(
                task_id="silver.s_person_address.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/s_person_address/s_person_address__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            (silver_h_person_sanction >> silver_s_person_demographic_sanction)
            (silver_h_person_sanction >> silver_s_person_address_sanction)
        with TaskGroup("Sanction_Sanction") as sanction_sanction:
            silver_h_sanction_sanction = SSHOperator(
                task_id="silver.h_sanction.sanction",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/h_sanction/h_sanction__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_s_sanction_person = SSHOperator(
                task_id="silver.s_sanction.person",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/s_sanction/s_sanction_person__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_l_sanction_person = SSHOperator(
                task_id="silver.l_sanction.person",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/l_sanction/l_sanction_person__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_s_sanction_company = SSHOperator(
                task_id="silver.s_sanction.company",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/s_sanction/s_sanction_company__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            silver_l_sanction_company = SSHOperator(
                task_id="silver.l_sanction.company",
                ssh_conn_id=ssh_spark_cluster,
                command=f"{entrypoint_cmd} \"python3 jobs/silver/l_sanction/l_sanction_company__src_sanction.py --payload '{str_payload}'\"",
                do_xcom_push=True,
                cmd_timeout=None,
                conn_timeout=None,
            )
            (silver_h_sanction_sanction >> silver_s_sanction_person)
            (silver_h_sanction_sanction >> silver_s_sanction_company)
            (
                [silver_h_sanction_sanction, silver_s_sanction_person]
                >> silver_l_sanction_person
            )
            (
                [silver_h_sanction_sanction, silver_s_sanction_company]
                >> silver_l_sanction_company
            )

    raw >> bronze >> silver
