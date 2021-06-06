from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from main import run_StackoverflowDatasetEtl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='stackoverflow_sample_dataset_etl',
    default_args=default_args,
    description="Get sample datasets from stack overflow data (bigQuery Dataset)",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
) as dag:
    # [START my_first_function]
    t1 = PythonOperator(
        task_id="stackoverflow_etl",
        python_callable=run_StackoverflowDatasetEtl
    )
    # [END my_first_function]

    t1 # noqa