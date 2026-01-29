import unittest
from db import _get_conn_params, dictfetchall, setup_db, load_test_data
from db import get_connection, get_cursor
from rnb import persist_to_remove


class TestDbSetup(unittest.TestCase):

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
                self.assertIn("gcms_territoire", table_names)

                self.assertEqual(len(table_names), 5)

                cursor.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname='processus_divers';"
                )
                tables = cursor.fetchall()
                table_names = [table[0] for table in tables]
                self.assertIn("rnb_to_remove", table_names)
                self.assertIn("rnb_last_changes", table_names)

                print(table_names)

                self.assertEqual(len(table_names), 2)

    def test_load_test_data(self):

        setup_db()
        load_test_data()

        with get_cursor() as cursor:

            q = "SELECT * FROM public.batiment_rnb_lien_bdtopo;"
            cursor.execute(q)
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 158)


class TestToRemoveInsertion(unittest.TestCase):

    def test_insert(self):

        setup_db()

        to_remove = ("rnb1", "rnb2", "rnb3")

        with get_cursor() as cursor:

            persist_to_remove(cursor, to_remove)

            q = "SELECT rnb_id FROM processus_divers.rnb_to_remove;"

            in_db = dictfetchall(cursor, q)
            self.assertEqual(len(in_db), 3)

            q = "SELECT * from processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation;"

            in_delete_table = dictfetchall(cursor, q)

            # We expect no rows in the delete table as we didn't have any matching rows in batiment_rnb_lien_bdtopo
            self.assertEqual(len(in_delete_table), 0)

    def test_with_data(self):

        setup_db()
        load_test_data()

        # Insert some data in batiment_rnb_lien_bdtopo
        with get_cursor() as cursor:

            # Rows in test data
            # RNB ID : 9CBEW6338EPA -> BAT_RNB_0000002444588925
            # RNB ID : 9CBJET9XEA36 -> BAT_RNB_0000002444588926
            # RNB ID : DUMMY_RNB_ID -> no matching row

            to_remove = ("9CBEW6338EPA", "9CBJET9XEA36", "DUMMY_RNB_ID")

            persist_to_remove(cursor, to_remove)

            q = "SELECT rnb_id FROM processus_divers.rnb_to_remove;"

            in_db = dictfetchall(cursor, q)
            self.assertEqual(len(in_db), 3)

            q = "SELECT * from processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation;"

            in_delete_table = dictfetchall(cursor, q)

            # We expect 2 rows in the delete table as we had 2 matching rows in batiment_rnb_lien_bdtopo
            # DUMMY_RNB_ID does not exist in batiment_rnb_lien_bdtopo
            self.assertEqual(len(in_delete_table), 2)

            for row in in_delete_table:

                self.assertIn(
                    row["cleabs"],
                    ("BAT_RNB_0000002444588925", "BAT_RNB_0000002444588926"),
                )


if __name__ == "__main__":
    unittest.main()
