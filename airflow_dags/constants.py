MINO_SERVER_END_POINT = "localhost:9000"
MINO_ACCESS_KEY = "minio"
MINO_SECRET_KEY = "password"
MINO_BUCKET_NAME = "transactions"
DB_NAME = 'swile'
SCHEMA_NAME = 'general'
PG_CONFIG = {
    # We're using default database to connect for the first time
    # We switch database to the correct one later
    "dbname": "postgres",
    "user": "postgres",
    "password": "toor",
    "host": "localhost"
}

TRANSACTIONS_STAGING_TABLE = 'transactions_staging'
INSEE_API_TOKEN = '<insert-token>'
