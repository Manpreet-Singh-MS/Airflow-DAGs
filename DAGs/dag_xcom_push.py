from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def person(age, ti):
    first_name = ti.xcom_pull(task_ids='get_detail', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_detail', key='last_name')
    occupation = ti.xcom_pull(task_ids='get_detail', key='occupation')
    # age = ti.xcom_pull(task_ids='get_detail', key='age')
    print(f"Hello my name is {first_name} {last_name} and my age is {age} years and i am a {occupation}.")


def get_detail(ti):
    ti.xcom_push(key='first_name', value='Manpreet')
    ti.xcom_push(key='last_name', value='Singh')
    ti.xcom_push(key='occupation', value='Data Engineer')
    # ti.xcom_push(key='age', value=27)
    


with DAG(
    dag_id="dag_xcom_push_value",
    description='',
    default_args=default_args,
    start_date=datetime(2023, 9, 6),
    schedule_interval='@daily'
)as dag:
    task1 = PythonOperator(
        task_id='xcom_pull',
        python_callable=person,
        op_kwargs={'age':27}
    )
    task2 = PythonOperator(
        task_id='get_detail',
        python_callable=get_detail
    )
    task2 >> task1
