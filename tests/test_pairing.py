import unittest
from db import dictfetchall, setup_db, load_test_data
from pairing import run_pairing_after_rnb_update


class TestPairing(unittest.TestCase):

    def test_pairing(self):

        setup_db()
        load_test_data()

        run_pairing_after_rnb_update()


if __name__ == "__main__":
    unittest.main()
