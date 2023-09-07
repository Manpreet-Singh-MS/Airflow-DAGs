from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'version':'Manpreet Singh',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = "dag_catchup",
    default_args=default_args,
    start_date = datetime(2023,9,7),
    schedule_interval='@daily',
    catchup=True

)as dag:
    task1 = BashOperator(
        task_id = 'task_catchup',
        bash_command = 'echo This is a a catchup DAG',
    )

    task1