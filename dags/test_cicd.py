from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="test_gitlab_cicd_flow",
    default_args=default_args,
    description="A simple DAG to test GitLab CI/CD deployment and validation",
    schedule_interval=None,  # Manually triggered for testing
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    test_task = PythonOperator(
        task_id="print_hello",
        python_callable=lambda: print("Hello, GitLab CI/CD is working!"),
        dag=dag,
    )

    end = DummyOperator(task_id="end", dag=dag)

    # Define task dependencies
    start >> test_task >> end
