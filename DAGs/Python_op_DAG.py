from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}


def Addition():
    a = 5
    b = 5
    print(a+b)


with DAG(
    dag_id='Python_Addition_DAG',
    default_args=default_args,
    description="Python DAG for Addition",
    start_date=datetime(2023, 9, 6),
    schedule_interval='@daily'

)as dag:
    task1 = PythonOperator(
        task_id='Python_Task',
        python_callable=Addition
    )

    task1
