# minimal test class
from datetime import datetime
import unittest

from rnb import (
    parse_sys_period,
    rnb_get_most_recent,
    calc_to_remove,
    remodel_rnb_to_last_changes,
)


class TestParseSysperiod(unittest.TestCase):
    def test_simple(self):

        sys_period = '["2023-12-08 11:44:32.395076+01","2023-12-22 12:52:29.558363+01")'
        start_date, end_date = parse_sys_period(sys_period)

        start_expected = datetime.fromisoformat("2023-12-08 11:44:32.395076+01:00")
        self.assertEqual(start_date, start_expected)

        end_expected = datetime.fromisoformat("2023-12-22 12:52:29.558363+01:00")
        self.assertEqual(end_date, end_expected)

    def test_just_start(self):

        sys_period = '["2023-12-24 17:19:12.971005+01",)'
        start_date, end_date = parse_sys_period(sys_period)

        start_expected = datetime.fromisoformat("2023-12-24 17:19:12.971005+01:00")
        self.assertEqual(start_date, start_expected)

        self.assertIsNone(end_date)

    def test_dummy(self):

        with self.assertRaises(ValueError):
            _, _ = parse_sys_period("wront_str")


class TestDiffSorting(unittest.TestCase):

    def test_rnb_get_most_recent(self):

        def iter_rows():
            rows = [
                {
                    "rnb_id": "id1",
                    "sys_period": '["2023-12-08 11:44:32.395076+01","2023-12-22 12:52:29.558363+01")',
                    "extra": "val1",
                },
                {
                    "rnb_id": "id1",
                    "sys_period": '["2023-12-22 12:52:29.558363+01","2023-12-24 17:19:12.971005+01")',
                    "extra": "val2",
                },
                {
                    "rnb_id": "id2",
                    "sys_period": '["2023-11-08 11:44:32.395076+01","2023-12-22 12:52:29.558363+01")',
                    "extra": "val3",
                },
                {
                    "rnb_id": "id2",
                    "sys_period": '["2023-12-22 12:52:29.558363+01","2023-12-24 17:19:12.971005+01")',
                    "extra": "val4",
                },
                {
                    "rnb_id": "id2",
                    "sys_period": '["2022-01-01 11:44:32.395076+01",)',
                    "extra": "val0",
                },
            ]
            for row in rows:
                yield row

        data = rnb_get_most_recent(iter_rows())

        # on doit avoir 2 éléments, un par rnb_id
        self.assertEqual(len(data), 2)

        # On test le premier item
        self.assertEqual(data[0]["rnb_id"], "id1")
        self.assertEqual(
            data[0]["sys_period"],
            '["2023-12-22 12:52:29.558363+01","2023-12-24 17:19:12.971005+01")',
        )
        self.assertEqual(data[0]["extra"], "val2")

        # On test le deuxième item
        self.assertEqual(data[1]["rnb_id"], "id2")
        self.assertEqual(
            data[1]["sys_period"],
            '["2023-12-22 12:52:29.558363+01","2023-12-24 17:19:12.971005+01")',
        )
        self.assertEqual(data[1]["extra"], "val4")


class TestRemoveCalc(unittest.TestCase):

    def test_simple(self):

        row = [
            {
                "rnb_id": "id1",
                "is_active": "0",
                "status": "constructionProject",
            },
            {
                "rnb_id": "id2",
                "is_active": "1",
                "status": "canceledConstructionProject",
            },
            {
                "rnb_id": "id3",
                "is_active": "1",
                "status": "demolished",
            },
            {
                "rnb_id": "id4",
                "is_active": "0",
                "status": "notUsable",
            },
        ]

        to_remove = calc_to_remove(row)

        expected = set(["id1", "id2", "id4"])

        self.assertEqual(to_remove, expected)


class TestRemodelRnbToLastChanges(unittest.TestCase):

    def test_mixed_actions(self):
        input_data = [
            {
                "rnb_id": "1",
                "action": "create",
                "sys_period": '["2023-01-01 10:00:00+00",)',
            },
            {
                "rnb_id": "2",
                "action": "update",
                "sys_period": '["2023-02-01 12:00:00+00",)',
            },
            {
                "rnb_id": "3",
                "action": "delete",  # treated as 'else' -> updated_at
                "sys_period": '["2023-03-01 14:00:00+00",)',
            },
        ]

        result = remodel_rnb_to_last_changes(input_data)

        self.assertEqual(len(result), 3)

        # 1. Create
        self.assertEqual(result[0]["created_at"], "2023-01-01T10:00:00+00:00")
        self.assertEqual(result[0]["updated_at"], "")

        # 2. Update
        self.assertEqual(result[1]["created_at"], "")
        self.assertEqual(result[1]["updated_at"], "2023-02-01T12:00:00+00:00")

        # 3. Delete (or other) -> handled as update in current logic
        self.assertEqual(result[2]["created_at"], "")
        self.assertEqual(result[2]["updated_at"], "2023-03-01T14:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
