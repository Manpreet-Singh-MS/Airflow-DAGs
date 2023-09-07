from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def person(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='First_name'),
    last_name = ti.xcom_pull(task_ids='get_name', key='Last_name')
    age = ti.xcom_pull(task_ids='get_age', key='Age')
    print(f"My Name is {first_name} {last_name} and my age is {age} years.")


def get_name(ti):
    ti.xcom_push(key='First_name',value='Manpreet')
    ti.xcom_push(key='Last_name',value='Singh')


def get_age(ti):
    ti.xcom_push(key='Age', value=27)


with DAG(
    dag_id='xcom_push_sample2',
    default_args=default_args,
    description="This xcom_push_sample2",
    start_date=datetime(2023, 9, 6),
    schedule_interval='@daily'
)as dag:
    task1 = PythonOperator(
        task_id='person',
        python_callable=person
    )
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1
