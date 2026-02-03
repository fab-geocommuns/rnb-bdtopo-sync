import unittest
from db import _get_conn_params, dictfetchall, setup_db, load_test_data
from db import get_connection, get_cursor
from rnb import (
    persist_to_remove,
    getDiff_RNB_from_file,
    _convert_rnb_diff,
    persist_last_changes,
)


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


class TestLastChangesInsertion(unittest.TestCase):

    def test_insert(self):

        setup_db()

        rnb_diff = getDiff_RNB_from_file("data/test_rnb_diff.csv")
        last_changes, _ = _convert_rnb_diff(rnb_diff)

        with get_cursor() as cursor:

            persist_last_changes(cursor, last_changes, "2025-06-01")

            q = "SELECT *, ST_AsEWKT(point) as point_wkt, ST_AsEWKT(shape) as shape_wkt FROM processus_divers.rnb_last_changes;"

            in_db = dictfetchall(cursor, q)
            self.assertEqual(len(in_db), 29)

            checked_row = next(row for row in in_db if row["rnb_id"] == "V8NJNTB7Z34Y")

            self.assertEqual(checked_row["action"], "update")
            self.assertEqual(checked_row["rnb_id"], "V8NJNTB7Z34Y")
            self.assertEqual(checked_row["status"], "constructed")
            self.assertEqual(checked_row["is_active"], "1")
            self.assertEqual(
                checked_row["sys_period"], '["2025-06-02 00:01:33.798109+00",)'
            )
            self.assertEqual(
                checked_row["point_wkt"],
                "SRID=4326;POINT(3.36679232876569 46.573599194397964)",
            )
            self.assertEqual(
                checked_row["shape_wkt"],
                "SRID=4326;MULTIPOLYGON(((3.3668926965814 46.57346470644805,3.366797265219796 46.57344790369021,3.366692119975701 46.57373368234788,3.366787545740529 46.57374958476431,3.3668926965814 46.57346470644805)))",
            )
            self.assertEqual(checked_row["addresses_id"], '["03321_zwwtvb_00006"]')
            self.assertEqual(
                checked_row["ext_id"],
                '[{"id": "bdnb-bc-5A6N-H6FX-ZTBF", "source": "bdnb", "created_at": "2023-12-07T13:28:43.229080+00:00", "source_version": "2023_01"}, {"id": "BATIMENT0000002200520272", "source": "bdtopo", "created_at": "2023-12-22T07:56:45.556905+00:00", "source_version": "bdtopo_2023_09"}]',
            )
            self.assertEqual(checked_row["parent_buildin"], "")
            self.assertEqual(
                checked_row["event_id"], "c8a78ee0-35e6-4ff5-8885-569ad694c941"
            )
            self.assertEqual(checked_row["created_at"], "")
            self.assertEqual(checked_row["updated_at"], "2025-06-02 00:01:33.798109+00")
            self.assertEqual(checked_row["event_type"], "update")


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
