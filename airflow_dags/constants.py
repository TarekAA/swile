MINO_SERVER_END_POINT = "localhost:9000"
MINO_ACCESS_KEY = "minio"
MINO_SECRET_KEY = "password"
MINO_BUCKET_NAME = "transactions"
DB_NAME = 'swile'
SCHEMA_NAME = 'general'

RUNNING_LOCALLY = True

SERVER_DIR_PATH = '/opt/airflow/dags/swile/'
LOCAL_RELATIVE_DIR_PATH = 'swile/'
DIR_PATH = LOCAL_RELATIVE_DIR_PATH if RUNNING_LOCALLY else SERVER_DIR_PATH

LOCAL_HOST = 'localhost'
SERVER_HOST = 'postgres-container'
HOST = LOCAL_HOST if RUNNING_LOCALLY else SERVER_HOST

LOCAL_PASS = 'toor'
SERVER_PASS = 'password'
PASS = LOCAL_PASS if RUNNING_LOCALLY else SERVER_PASS

PG_CONFIG = {
    # We're using default database to connect for the first time
    # We switch database to the correct one later
    "dbname": "postgres",
    "user": "postgres",
    "password": PASS,
    "host": HOST
}

TRANSACTIONS_STAGING_TABLE = 'transactions_staging'
INSEE_API_TOKEN = '<insert-token>'
