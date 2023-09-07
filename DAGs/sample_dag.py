from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'MASRP',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='This is a first dag',
    start_date=datetime(2023, 9, 5, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo Hello world, This is the first task"
    )
    task1
