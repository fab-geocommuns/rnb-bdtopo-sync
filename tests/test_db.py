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

                # Test extensions creation
                cursor.execute("SELECT extname FROM pg_extension")
                extensions = cursor.fetchall()
                self.assertIn("postgis", [ext[0] for ext in extensions])

                # Test schemas creation
                cursor.execute("SELECT nspname FROM pg_namespace")
                schemas = cursor.fetchall()
                self.assertIn("processus_divers", [schema[0] for schema in schemas])

                # Test tables creation
                cursor.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public';"
                )
                tables = cursor.fetchall()
                table_names = [table[0] for table in tables]
                self.assertIn("batiment_rnb_lien_bdtopo", table_names)
                self.assertIn("batiment", table_names)
                self.assertIn("staging_batiment_csv", table_names)
                self.assertIn("batiment_rnb_lien_bdtopo", table_names)
                self.assertIn("gcms_territoires", table_names)


if __name__ == "__main__":
    unittest.main()
