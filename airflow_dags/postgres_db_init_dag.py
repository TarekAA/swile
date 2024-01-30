from swile.scripts.prepare_postgresql import *

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Define the Python functions that will be used as tasks


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def init_postgres_db_task():
    safe_execute("CREATE DATABASE swile")
    safe_execute("CREATE SCHEMA  general", update_config={'dbname': DB_NAME})
    run_dbt_project(create_transaction_staging_commands)
    run_dbt_project(create_siret_naf_staging)


with DAG(
        'postgres_db_init_dag',
        default_args=default_args,
        description='DAG that will run once in order to init postgres db, e.g., create tables, schemas, etc',
        schedule_interval="@once",
        max_active_runs=1,
        catchup=False
) as dag:
    start_task = DummyOperator(task_id='start', dag=dag)

    init_postgres_db_task = PythonOperator(
        task_id='init_postgres_db',
        python_callable=init_postgres_db_task,
        op_kwargs={
            "bucket_name": MINO_BUCKET_NAME,
            "file_name": "{{ ds }}.json",
            "table_name": TRANSACTIONS_STAGING_TABLE
        }
    )

    end_task = DummyOperator(task_id='end', dag=dag)

    start_task >> init_postgres_db_task >> end_task
