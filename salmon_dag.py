from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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
    dag_id='salmon',
    default_args=default_args,
    description="Our first DAG with ETL Process!",
    schedule_interval=None,
    start_date=days_ago(2),
) as dag:
    # [START my_first_function]
    def my_first_function():
        print("Kuliah Kilat Airflow 1")

    t1 = PythonOperator(
        task_id="First",
        python_callable=my_first_function
    )
    # [END my_first_function]

    # [START my_second_function]
    def my_second_function():
        print("Kuliah Kilat Airflow 2")


    t2 = PythonOperator(
        task_id="Second",
        python_callable=my_second_function
    )
    # [END my_second_function]

    t1 >> t2