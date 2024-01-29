import json
import psycopg2
from minio import Minio
from swile.airflow_dags.constants import *
from dbt.cli.main import dbtRunner, dbtRunnerResult


def obtain_mino_client():
    mc = Minio(
        MINO_SERVER_END_POINT,
        access_key=MINO_ACCESS_KEY,
        secret_key=MINO_SECRET_KEY,
        secure=False
    )
    return mc


def obtain_json_file_from_bucket(bucket_name, file_name, mc: Minio = None):
    mc = obtain_mino_client() if not mc else mc
    str_data_encoded = mc.get_object(bucket_name, file_name).read().decode('utf-8')
    json_data = json.loads(str_data_encoded)
    return json_data


def insert_into_postgres(bucket_name: str, file_name: str, table_name: str):
    # Obtain JSON data from MinIO
    mino_client = obtain_mino_client()
    data = obtain_json_file_from_bucket(bucket_name, file_name, mino_client)
    print(data)

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
    # initialize
    dbt = dbtRunner()

    if not cli_args:
        cli_args = ["run", "--project-dir", "swile/dbt/swile",
                    "--profiles-dir", "swile/"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        if hasattr(r, 'node'):
            print(f"{r.node.name}: {r.status}")
        else:
            print(f"operation-node: {r.status}")


if __name__ == '__main__':
    # This part is for manually loading a file from MinIO into postgresql
    object_name = "2023-10-04.json"

    # Obtain JSON file and insert it into staging table
    insert_into_postgres(MINO_BUCKET_NAME, object_name, table_name=TRANSACTIONS_STAGING_TABLE)

    # Move data from staging into final table

    run_dbt_project()
