import psycopg2
from swile.airflow_dags.constants import *
from swile.airflow_dags.load_event_data import run_dbt_project

create_transaction_staging_commands = ["run-operation", "create_transaction_staging_table",
                                       "--args", '{"table_name": "transactions_staging"}',
                                       "--project-dir", f"{DIR_PATH}dbt/swile",
                                       "--profiles-dir", f"{DIR_PATH}"]

create_siret_naf_staging = ["run-operation", "create_siret_naf_staging_table",
                            "--args", '{"table_name": "siret_naf_staging"}',
                            "--project-dir", f"{DIR_PATH}dbt/swile",
                            "--profiles-dir", f"{DIR_PATH}"]


def safe_execute(command, conn=None, cur=None, update_config=None, close=True):
    if update_config is None:
        update_config = {}

    new_config = dict(**PG_CONFIG)
    new_config.update(update_config)
    conn = psycopg2.connect(**new_config) if not conn else conn
    conn.autocommit = True
    cur = conn.cursor() if not cur else cur
    try:
        cur.execute(command)
    except psycopg2.Error as e:
        print(e)
    finally:
        conn.commit()
        if close:
            cur.close()
            conn.close()
        else:
            return conn, cur


if __name__ == "__main__":
    # todo Replace database and schema creation from connector with DBT pre-hooks
    safe_execute("CREATE DATABASE swile")
    safe_execute("CREATE SCHEMA  general", update_config={'dbname': DB_NAME})
    run_dbt_project(create_transaction_staging_commands)
    run_dbt_project(create_siret_naf_staging)
