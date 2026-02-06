import unittest
from db import dictfetchall, setup_db, load_test_data, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


class TestPairing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_db()

    def test_semantic(self):
        """Test semantic pairing when RNB building contains BD TOPO cleabs in ext_ids."""

        # Create a BD TOPO building
        bdtopo_polygon_wkt = "MULTIPOLYGON Z(((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)))"
        bdtopo_cleabs = create_bdtopo_building(polygon_wkt=bdtopo_polygon_wkt)

        # Create an RNB building with the BD TOPO cleabs in ext_ids
        rnb_polygon_geojson = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        }
        rnb_identifiant = "RNB_TEST_001"
        create_rnb_building(
            polygon_geojson=rnb_polygon_geojson,
            identifiant_rnb=rnb_identifiant,
            bdtopo_ids=[bdtopo_cleabs]
        )

        # Run the pairing process
        run_pairing_after_rnb_update()

        # Check that semantic pairing occurred in the processed staging table
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (rnb_identifiant,)
                )

        # Assertions
        self.assertEqual(len(results), 1, "Expected exactly one RNB building")
        result = results[0]
        self.assertEqual(result["identifiant_rnb"], rnb_identifiant)
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs,
                        "RNB building should be linked to BD TOPO building")
        self.assertEqual(result["traitement"], "Croisement sémantique",
                        "Pairing should be semantic")
