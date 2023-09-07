from airflow.decorators import dag,task
from datetime import datetime,timedelta

default_args = {
    'version':'Manpreet Singh',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id = "Dag_Task_Flow_API_2",
    default_args = default_args,
    start_date= datetime(2023,9,7),
    schedule_interval= '@daily'
)

def taskflow_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Manpreet',
            'last_name':'Singh'
        }
    
    @task()
    def get_age():
        return 27
    
    @task()
    def greetings(first_name,last_name,age):
        print(f"hello my name is {first_name} {last_name} and i am {age} years old.")
    
    name_dict = get_name()
    age = get_age()
    greetings(first_name=name_dict['first_name'],
              last_name=name_dict['last_name'],
              age=age)
    
taskflow_api = taskflow_etl()