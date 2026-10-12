import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="nyc_taxi_smoke_test",
    description="Verify that the local NYC Taxi Airflow environment works",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "test"],
) as dag:

    print_environment = BashOperator(
        task_id="print_environment",
        bash_command="""
            echo "Airflow is running"
            echo "Execution date: {{ ds }}"
            python --version
        """,
    )

    print_environment