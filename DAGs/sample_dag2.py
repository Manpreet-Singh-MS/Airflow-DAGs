from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'MASRP',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='second_dag',
    default_args=default_args,
    description='This is a second dag',
    start_date=datetime(2023, 9, 5, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='second_task',
        bash_command="echo Hello world, This is the second task"
    )
    task1
