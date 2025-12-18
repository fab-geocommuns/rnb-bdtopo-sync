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
