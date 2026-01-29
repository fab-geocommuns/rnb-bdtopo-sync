import os
import psycopg2
from contextlib import contextmanager
from utils import load_env


SCHEMA_NAME = "processus_divers"


def _get_conn_params() -> dict:

    load_env()

    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PWD"),
        "database": os.getenv("DB_NAME"),
    }


@contextmanager
def get_connection():
    params = _get_conn_params()
    conn = psycopg2.connect(**params)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(conn=None):
    if conn is None:
        with get_connection() as conn:
            with conn:
                with conn.cursor() as cursor:
                    yield cursor
    else:
        with conn:
            with conn.cursor() as cursor:
                yield cursor


def setup_db():

    with get_connection() as conn:
        with get_cursor(conn) as cursor:

            # Create schemas (execute the init-schemas.sql file)
            cursor.execute(open("db/init/init-schemas.sql", "r").read())

            # Create extensions (execute the init-extensions.sql file)
            cursor.execute(open("db/init/init-extensions.sql", "r").read())

            # Create tables (execute the init-tables.sql file)
            cursor.execute(open("db/init/init-tables.sql", "r").read())

            # Create functions (execute the init-functions.sql file)
            cursor.execute(open("db/init/init-functions.sql", "r").read())

    # # create role invite if not exists
    # cursor.execute(
    #     f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'invite') THEN CREATE ROLE invite NOINHERIT LOGIN; END IF; END $$;"
    # )
