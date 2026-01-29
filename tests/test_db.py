import unittest
from db import _get_conn_params, setup_db
from db import get_connection, get_cursor


class TestDb(unittest.TestCase):

    def test_params(self):

        params = _get_conn_params()

        expected = {
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "postgres",
            "database": "rnb",
        }

        self.assertDictEqual(params, expected)

    def test_setup_db(self):

        setup_db()

        # check if the schemas and extensions are created
        with get_connection() as conn:
            with get_cursor(conn) as cursor:

                cursor.execute("SELECT extname FROM pg_extension")
                extensions = cursor.fetchall()
                self.assertIn("postgis", [ext[0] for ext in extensions])

                cursor.execute("SELECT nspname FROM pg_namespace")
                schemas = cursor.fetchall()
                self.assertIn("processus_divers", [schema[0] for schema in schemas])


if __name__ == "__main__":
    unittest.main()
