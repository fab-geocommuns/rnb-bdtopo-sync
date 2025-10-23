# minimal test class
from datetime import datetime
import unittest

from rnb import extract_start_date, rnb_get_most_recent


class TestParseSysperiod(unittest.TestCase):
    def test_simple(self):

        sys_period = '["2023-12-08 11:44:32.395076+01","2023-12-22 12:52:29.558363+01")'
        start_date = extract_start_date(sys_period)

        expected = datetime.fromisoformat("2023-12-08 11:44:32.395076+01:00")

        self.assertEqual(start_date, expected)

    def test_just_start(self):

        sys_period = '["2023-12-24 17:19:12.971005+01",)'
        start_date = extract_start_date(sys_period)

        expected = datetime.fromisoformat("2023-12-24 17:19:12.971005+01:00")

        self.assertEqual(start_date, expected)

    def test_dummy(self):

        sys_period = "wront_str"
        start_date = extract_start_date(sys_period)

        self.assertIsNone(start_date)


class TestDiffSorting(unittest.TestCase):

    def test_simple(self):

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


if __name__ == "__main__":
    unittest.main()
