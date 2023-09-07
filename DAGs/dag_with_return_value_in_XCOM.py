from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def get_name():
    return 'Manpreet Singh'


with DAG(
    dag_id='Return_value_xcom',
    default_args=default_args,
    description='Returns value to xcom',
    start_date=datetime(2023, 9, 6),
    schedule_interval='@daily'


)as dag:
    task1 = PythonOperator(
        task_id='Return_Value_XCOM',
        python_callable=get_name
    )
    task1
