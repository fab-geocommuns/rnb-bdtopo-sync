# minimal test class
import unittest

from rnb import extract_start_date


class TestParseSysperiod(unittest.TestCase):
    def test_simple(self):

        sys_period = '["2023-12-08 11:44:32.395076+01","2023-12-22 12:52:29.558363+01")'
        start_date = extract_start_date(sys_period)

        self.assertEqual(start_date, "2023-12-08 11:44:32.395076+01")


if __name__ == "__main__":
    unittest.main()
