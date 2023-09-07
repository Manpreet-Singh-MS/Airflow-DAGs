from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Manpreet Singh',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}
def person(ti,age):
    name = ti.xcom_pull(task_ids = 'get_name')
    print("My name is {} and my age is {}.".format(name,age))

def get_name():
    return 'Manpreet Singh'

with DAG(
    dag_id = 'xcom_dependency',
    default_args = default_args,
    description = 'Xcom_dependency on tasks',
    start_date = datetime(2023,9,6),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = "person",
        python_callable = person,
        op_kwargs={'age':27}
    )
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name
    )

    task2 >> task1

   