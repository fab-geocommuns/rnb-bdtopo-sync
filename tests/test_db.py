import unittest
from db import _get_conn_params


class TestDb(unittest.TestCase):

    def test_params(self):

        params = _get_conn_params()

        expected = {
            "host": "test_host",
            "port": "test_1234",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db_name",
        }

        self.assertDictEqual(params, expected)


if __name__ == "__main__":
    unittest.main()
