from airflow.decorators import dag, task
from datetime import datetime,timedelta

default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='Taskflow_API',
    default_args=default_args,
    start_date=datetime(2023, 9, 6),
    schedule_interval='@daily')
def hello_world_etl():
    
    @task()
    def get_name():
        return 'Manpreet'

    @task()
    def get_age():
        return 27

    @task()
    def greetings(name, age):
        print(f"Hello my name is {name} and my age is {age} years old")

    name = get_name()
    age = get_age()
    greetings(name=name, age=age) 

hello_world_etl()
