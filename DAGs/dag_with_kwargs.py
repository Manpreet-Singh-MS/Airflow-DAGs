from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

default_args={
    'owner':'Manpreet Singh',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def person(name,age):
    print("My Name is {} and my age is {}.".format(name,age))


with DAG(
    dag_id = 'Python_kwargs',
    default_args = default_args,
    description = 'DAG with kwargs',
    start_date = datetime(2023,9,6),
    schedule_interval ='@daily'

)as dag:
    task1 = PythonOperator(
        task_id = 'Python_Kwargs',
        python_callable=person,
        op_kwargs = {'name': 'Manpreet','age':27} 
    )
    task1