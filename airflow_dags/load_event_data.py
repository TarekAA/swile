"""
scrit that contain logic to load data and move it to prod table.
Runs DBT commands
"""

import json
import psycopg2
from minio import Minio
from swile.airflow_dags.constants import *
from dbt.cli.main import dbtRunner, dbtRunnerResult
import argparse
import typing


def obtain_mino_client() -> Minio:
    """Obtain MinIO client using configuration found in constants.py

    :return: Minio client
    """
    mc = Minio(
        MINO_SERVER_END_POINT,
        access_key=MINO_ACCESS_KEY,
        secret_key=MINO_SECRET_KEY,
        secure=False
    )
    return mc


def obtain_json_file_from_bucket(bucket_name: str, file_name: str, mc: Minio = None) -> typing.Any:
    """ Obtain JSON file from MinIO bucket

    :param bucket_name: str, bucket_name where the objetct / file resides
    :param file_name: str, object / file_name inside the bucket
    :param mc: MinIO client. If None initialize client from constatnts.py
    :return:  json_data, dictionary containing obtained json data. We assume all objects to be in json format
    """
    mc = obtain_mino_client() if not mc else mc
    str_data_encoded = mc.get_object(bucket_name, file_name).read().decode('utf-8')
    json_data = json.loads(str_data_encoded)
    return json_data


def insert_into_postgres(bucket_name: str, file_name: str, table_name: str):
    """ Connect to MinIO and obtain json event data from a file, then
    insert all content into postgres database using psycopg2 connector

    :param bucket_name: str, bucket_name where the objetct / file resides
    :param file_name: str, object / file_name inside the bucket
    :param table_name: str, table to write to
    :return: None
    """
    # Obtain JSON data from MinIO
    mino_client = obtain_mino_client()
    data = obtain_json_file_from_bucket(bucket_name, file_name, mino_client)
    # print(data)

    # Insert JSON data in postgresql

    # switch database from default one
    pg_config = dict(**PG_CONFIG)
    pg_config['dbname'] = DB_NAME
    # Connect to PostgreSQL
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()

    for item in data:
        id_value = item.get('id')
        type_value = item.get('type')
        amount_value = item.get('amount')
        status_value = item.get('status')
        created_at_value = item.get('created_at')
        wallet_id_value = item.get('wallet_id')
        siret_value = item.get('siret')

        cur.execute(
            f"INSERT INTO general.{table_name} "
            "(id, type, amount, status, created_at, wallet_id, siret) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (id_value, type_value, amount_value, status_value, created_at_value, wallet_id_value, siret_value))

    conn.commit()
    cur.close()
    conn.close()


def run_dbt_project(cli_args: list[str] = None):
    """ Run DBT project or portion of the DBT project.
    Command args should be passed, specially --project-dir and --profiles-dir.
    Other options are optionals but recommended such as --select.

    If no args were passed run the entire DBT project with arguments from constatnts.py

    :param cli_args: list[str], args to be passed to DBT command
    :return: None
    """
    # initialize
    dbt = dbtRunner()

    if not cli_args:
        cli_args = ["run", "--project-dir", f"{DIR_PATH}dbt/swile",
                    "--profiles-dir", f"{DIR_PATH}"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)
    print(res)
    # inspect the results
    for r in res.result:
        if hasattr(r, 'node'):
            print(f"{r.node.name}: {r.status}")
        else:
            print(f"operation-node: {r.status}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='insert transaction data into database')
    parser.add_argument('--object_name', type=str, help='The event data file_name / date, no format.')
    args = parser.parse_args()
    # This part is for manually loading a file from MinIO into postgresql
    object_name = "2023-10-01.json" if not args.object_name else f"{args.object_name}.json"

    # Obtain JSON file and insert it into staging table
    insert_into_postgres(MINO_BUCKET_NAME, object_name, table_name=TRANSACTIONS_STAGING_TABLE)

    # Move data from staging into final table

    cli_args = ["run",
                "--select", "transactions",
                "--project-dir", F"{DIR_PATH}dbt/swile",
                "--profiles-dir", f"{DIR_PATH}"]
    run_dbt_project(cli_args)
