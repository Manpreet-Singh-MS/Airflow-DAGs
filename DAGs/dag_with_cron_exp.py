from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='DAG_With_Cron_Exp',
    default_args=default_args,
    start_date=datetime(2023, 9, 7),
    schedule_interval='0 3 * * Tue-Fri'


)as dag:
    task1 = BashOperator(
        task_id='DAG_with_Cron',
        bash_command='echo  This is the DAG with cron expression'

    )
    task1
