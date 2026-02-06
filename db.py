import datetime
import os
import psycopg2
from contextlib import contextmanager
from utils import load_env
from datetime import datetime


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

            _drop_tables(cursor)

            # Create schemas (execute the init-schemas.sql file)
            with open("db/init/init-schemas.sql", "r") as f:
                cursor.execute(f.read())

            # Create extensions (execute the init-extensions.sql file)
            with open("db/init/init-extensions.sql", "r") as f:
                cursor.execute(f.read())

            # Create tables (execute the init-tables.sql file)
            with open("db/init/init-tables.sql", "r") as f:
                cursor.execute(f.read())

            # Create roles if they don't exist
            cursor.execute(
                "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pbm') THEN CREATE ROLE pbm; END IF; END $$;"
            )
            cursor.execute(
                "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'recserveur') THEN CREATE ROLE recserveur; END IF; END $$;"
            )
            cursor.execute(
                "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'invite') THEN CREATE ROLE invite NOINHERIT LOGIN; END IF; END $$;"
            )

            # Create functions (execute the init-functions.sql file)
            with open("db/init/init-functions.sql", "r") as f:
                cursor.execute(f.read())

            today = datetime.now().strftime("%Y-%m-%d")

            _create_to_remove_table(cursor, today)
            _create_last_changes_table(cursor, today)


def _create_to_remove_table(cursor, table_creation_date):

    cursor.execute(f"DROP TABLE IF EXISTS processus_divers.rnb_to_remove cascade;")

    create_sql = f"""\
    CREATE TABLE IF NOT EXISTS processus_divers.rnb_to_remove (rnb_id varchar NULL);\
	CREATE UNIQUE INDEX "rnb_to_remove_rnb_id_pkey" ON processus_divers.rnb_to_remove USING btree (rnb_id);\
    GRANT SELECT ON processus_divers.rnb_to_remove TO invite;\
    COMMENT ON TABLE processus_divers.rnb_to_remove IS %(table_creation_comment)s;
    """

    cursor.execute(
        create_sql,
        {"table_creation_comment": f"Ne pas supprimer - {table_creation_date}"},
    )


def _create_last_changes_table(cursor, table_creation_date):

    cursor.execute(f"DROP TABLE IF EXISTS processus_divers.rnb_last_changes cascade")

    create_sql = f"""\
        CREATE TABLE IF NOT EXISTS processus_divers.rnb_last_changes (action varchar NULL,\
            rnb_id varchar NULL,\
            status varchar NULL,\
            is_active varchar NULL,\
            sys_period varchar NULL,\
            point public.geometry NULL,\
            shape public.geometry NULL,\
            addresses_id varchar NULL,\
            ext_ids varchar NULL,\
            parent_buildings varchar NULL,\
            event_id varchar NULL,\
            created_at varchar NULL,\
            updated_at varchar NULL,\
            event_type varchar NULL);\
        CREATE UNIQUE INDEX "rnb_last_changes_rnb_id_pkey" ON processus_divers.rnb_last_changes USING btree (rnb_id);\
        CREATE INDEX "rnb_last_changes_POINT_idx" ON processus_divers.rnb_last_changes USING gist (point);\
        GRANT SELECT ON processus_divers.rnb_last_changes TO invite;\
        CREATE INDEX rnb_last_changes_shape_idx ON processus_divers.rnb_last_changes USING gist (shape);\
        COMMENT ON TABLE processus_divers.rnb_last_changes IS %(table_creation_comment)s;\
        """
    cursor.execute(
        create_sql,
        {"table_creation_comment": f"Ne pas supprimer - {table_creation_date}"},
    )


def _drop_tables(cursor):

    tables_to_drop = [
        "processus_divers.rnb_to_remove",
        "processus_divers.rnb_last_changes",
        "processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation",
        # Work tables created by pairing functions
        "processus_divers.rnb_batiments_rnb_restant_creation",
        "processus_divers.rnb_batiments_rnb_buffer_creation",
        "processus_divers.rnb_batiments_bduni_restant_creation",
        "processus_divers.rnb_batiments_rnb_traites_creation",
        "processus_divers.rnb_batiments_bduni_traites_creation",
        "processus_divers.rnb_croisements_creation",
        "processus_divers.update_batiment_rnb_lien_bdtopo__batiment_rnb_creation",
        "processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_creation",
        "processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr",
        "processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage",
        "processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_demolished",
        "processus_divers.insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage",
        "processus_divers.rnb_fusionnees",
        "processus_divers.stat_rnb",
        "processus_divers.batiment_suppr",
        "processus_divers.lien_rnb_bdtopo",
        "processus_divers.cleabs_rnb_a_reprendre",
        "processus_divers.rnb_batiments_bduni_buffer_creation",
    ]

    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")


def dictfetchall(cursor, query, params=None):
    cursor.execute(query, params)
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def load_test_data():
    with get_connection() as conn:
        with get_cursor(conn) as cursor:

            # RNB lien data
            with open("data/test_batiment_rnb_lien_bdtopo.csv", "r") as f:
                cursor.copy_expert(
                    "COPY public.batiment_rnb_lien_bdtopo FROM STDIN WITH CSV HEADER",
                    f,
                )

            # BD Topo building to match
            with open("data/test_appariement_bdtopo.csv", "r") as f:
                cursor.copy_expert(
                    "COPY public.staging_batiment_csv FROM STDIN WITH CSV HEADER",
                    f,
                )
            with open("db/init/load-batiment.sql", "r") as f:
                cursor.execute(f.read())
