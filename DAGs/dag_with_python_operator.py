from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet():
    print('Hello world This is airflow dag with python')


with DAG(
    default_args=default_args,
    dag_id='Python_v01',
    description='This is sample dag using python operator',
    start_date=datetime(2023, 9, 6),
    schedule_interval='@daily'



) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task1
