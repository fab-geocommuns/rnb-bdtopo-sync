import os
import psycopg2
from utils import load_env


def _get_conn_params() -> dict:

    load_env()

    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PWD"),
        "database": os.getenv("DB_NAME"),
    }


def get_cursor():

    params = _get_conn_params()

    conn = psycopg2.connect(**params)
    try:
        with conn:
            with conn.cursor() as cursor:
                yield cursor
    finally:
        conn.close()
