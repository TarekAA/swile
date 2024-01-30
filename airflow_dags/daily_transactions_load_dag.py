# from airflow import DAG
# from airflow.operators.python import PythonOperator
from swile.airflow_dags.load_event_data import *
from swile.airflow_dags.curate_naf_code import *

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

with DAG(
        'daily_transactions_load_dag',
        default_args=default_args,
        description='DAG that will load daily transactions in postgres,'
                    ' combine data from different sources and calculate spent.',
        schedule_interval="@daily",
        max_active_runs=1,
        catchup=False
) as dag:
    start_task = DummyOperator(task_id='start', dag=dag)

    insert_transaction_into_postgres_task = PythonOperator(
        task_id='insert_transaction_into_postgres',
        python_callable=insert_into_postgres,
        op_kwargs={
            "bucket_name": MINO_BUCKET_NAME,
            "file_name": "{{ ds }}.json",
            "table_name": TRANSACTIONS_STAGING_TABLE
        }
    )

    merge_into_final_transactions_task = PythonOperator(
        task_id='merge_into_final_transactions',
        python_callable=run_dbt_project,
        op_kwargs={
            "cli_args": ["run",
                         "--select", "transactions",
                         "--project-dir", f"{DIR_PATH}dbt/swile",
                         "--profiles-dir", f"{DIR_PATH}"]
        }
    )

    generate_siret_to_be_mapped_task = PythonOperator(
        task_id='generate_siret_to_be_mapped',
        python_callable=generate_siret_to_be_mapped_table,
        op_kwargs={}
    )

    write_naf_codes_task = PythonOperator(
        task_id='write_naf_codes',
        python_callable=write_naf_codes,
        op_kwargs={}
    )

    generate_siret_naf_table_task = PythonOperator(
        task_id='generate_siret_naf_table',
        python_callable=generate_siret_naf_table,
        op_kwargs={}
    )

    generate_spent_table_task = PythonOperator(
        task_id='generate_spent_table',
        python_callable=generate_spent_table,
        op_kwargs={}
    )

    end_task = DummyOperator(task_id='end', dag=dag)

    start_task >> insert_transaction_into_postgres_task >> merge_into_final_transactions_task
    merge_into_final_transactions_task >> generate_siret_to_be_mapped_task
    generate_siret_to_be_mapped_task >> write_naf_codes_task >> generate_siret_naf_table_task
    generate_siret_naf_table_task >> generate_spent_table_task >> end_task
