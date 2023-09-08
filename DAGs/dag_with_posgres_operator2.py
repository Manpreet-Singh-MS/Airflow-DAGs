from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Manpreet Singh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="DAG_with_postgres_operator2",
    default_args=default_args,
    start_date=datetime(2023, 9, 6),
    schedule_interval='0 0 * * *'
)as dag:
    task1 = PostgresOperator(
        task_id='Postgres_operator_create_db',
        postgres_conn_id='postgres_localhost_1',
        sql="""
            create table if not exists dag_runs_1(
                dt date,
                dag_id character varying,
                primary key(dt,dag_id)
            )
            """
    )
    task2 = PostgresOperator(
        task_id='Postgres_operator_insert_record',
        postgres_conn_id='postgres_localhost_1',
        sql="""
            insert into dag_runs_1(dt,dag_id) values ('{{ds}}','{{dag.dag_id}}')
            
            """
    )

    task1 >> task2
