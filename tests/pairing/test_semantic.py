import unittest
from db import dictfetchall, setup_db, load_test_data, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


# WGS84 coordinates near Orléans (dep 45)
# At lat ~47.9: 10m east ≈ +0.000134° lon, 10m north ≈ +0.000090° lat
BASE_LON = 1.900000
BASE_LAT = 47.900000

# Standard ~10x10m house polygon
HOUSE_10x10 = {
    "type": "Polygon",
    "coordinates": [
        [
            [BASE_LON, BASE_LAT],
            [BASE_LON + 0.000134, BASE_LAT],
            [BASE_LON + 0.000134, BASE_LAT + 0.000090],
            [BASE_LON, BASE_LAT + 0.000090],
            [BASE_LON, BASE_LAT],
        ]
    ],
}


class TestSemantic(unittest.TestCase):

    def setUp(self):
        """Reset database before each test to ensure clean state."""
        setup_db()

    def test_simple(self):
        """Test semantic pairing when RNB building contains BD TOPO cleabs in ext_ids."""

        # Create a BD TOPO building
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Create an RNB building with the BD TOPO cleabs in ext_ids
        rnb_identifiant = "RNB_TEST_001"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_identifiant,
            bdtopo_ids=[bdtopo_cleabs],
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
                    (rnb_identifiant,),
                )

        # Assertions
        self.assertEqual(len(results), 1, "Expected exactly one RNB building")
        result = results[0]
        self.assertEqual(result["identifiant_rnb"], rnb_identifiant)
        self.assertEqual(
            result["liens_vers_batiment"],
            bdtopo_cleabs,
            "RNB building should be linked to BD TOPO building",
        )
        self.assertEqual(
            result["traitement"], "Croisement sémantique", "Pairing should be semantic"
        )

    def test_with_destroyed_bdtopo(self):
        """Test that no pairing occurs when BD TOPO building is marked as destroyed."""

        # Create a BD TOPO building marked as destroyed
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson=HOUSE_10x10, gcms_detruit=True
        )

        # Create an RNB building with the destroyed BD TOPO cleabs in ext_ids
        rnb_identifiant = "RNB_TEST_DESTROYED"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_identifiant,
            bdtopo_ids=[bdtopo_cleabs],
        )

        # Run the pairing process
        run_pairing_after_rnb_update()

        # Check that NO pairing occurred
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                # Check if the RNB building was processed
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (rnb_identifiant,),
                )

                # Also check that the destroyed BD TOPO building was NOT included in candidates
                bdtopo_candidates = dictfetchall(
                    cursor,
                    """
                    SELECT COUNT(*) as cnt
                    FROM processus_divers.rnb_batiments_bduni_restant_creation
                    WHERE cleabs = %s
                    """,
                    (bdtopo_cleabs,),
                )

                bdtopo_processed = dictfetchall(
                    cursor,
                    """
                    SELECT COUNT(*) as cnt
                    FROM processus_divers.rnb_batiments_bduni_traites_creation
                    WHERE cleabs = %s
                    """,
                    (bdtopo_cleabs,),
                )

        # Assertions
        self.assertEqual(
            len(results), 1, "Expected exactly one RNB building to be processed"
        )
        result = results[0]
        self.assertEqual(result["identifiant_rnb"], rnb_identifiant)
        self.assertEqual(
            result["liens_vers_batiment"],
            "",
            "RNB building should NOT be linked (destroyed BD TOPO)",
        )
        self.assertEqual(
            result["traitement"],
            "Bâtiment non apparié, à plus de 20m d'un autre bâtiment BDUni",
            "RNB building should be marked as non-paired",
        )

        # Verify destroyed BD TOPO building was excluded from candidates
        self.assertEqual(
            bdtopo_candidates[0]["cnt"],
            0,
            "Destroyed BD TOPO should not be in candidate list",
        )
        self.assertEqual(
            bdtopo_processed[0]["cnt"],
            0,
            "Destroyed BD TOPO should not be in processed list",
        )

    def test_multiple_bdtopo_in_ext_ids(self):
        """Test semantic pairing when RNB building has multiple BD TOPO cleabs in ext_ids (1 RNB → N BD TOPO)."""

        # Create 3 BD TOPO buildings spaced ~30m apart (within the 100m buffer)
        bdtopo_cleabs_1 = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        bdtopo_cleabs_2 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000402, BASE_LAT],
                        [BASE_LON + 0.000536, BASE_LAT],
                        [BASE_LON + 0.000536, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000402, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000402, BASE_LAT],
                    ]
                ],
            }
        )

        bdtopo_cleabs_3 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000804, BASE_LAT],
                        [BASE_LON + 0.000938, BASE_LAT],
                        [BASE_LON + 0.000938, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000804, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000804, BASE_LAT],
                    ]
                ],
            }
        )

        # Create an RNB building covering all three
        rnb_polygon_geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [BASE_LON, BASE_LAT],
                    [BASE_LON + 0.000938, BASE_LAT],
                    [BASE_LON + 0.000938, BASE_LAT + 0.000090],
                    [BASE_LON, BASE_LAT + 0.000090],
                    [BASE_LON, BASE_LAT],
                ]
            ],
        }
        rnb_identifiant = "RNB_TEST_MULTIPLE"
        create_rnb_building(
            polygon_geojson=rnb_polygon_geojson,
            identifiant_rnb=rnb_identifiant,
            bdtopo_ids=[bdtopo_cleabs_1, bdtopo_cleabs_2, bdtopo_cleabs_3],
        )

        # Run the pairing process
        run_pairing_after_rnb_update()

        # Check that semantic pairing occurred with all three BD TOPO buildings
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (rnb_identifiant,),
                )

        # Assertions
        self.assertEqual(len(results), 1, "Expected exactly one RNB building")
        result = results[0]
        self.assertEqual(result["identifiant_rnb"], rnb_identifiant)
        self.assertEqual(
            result["traitement"], "Croisement sémantique", "Pairing should be semantic"
        )

        # Check that all three BD TOPO cleabs are in the result (order may vary)
        liens = result["liens_vers_batiment"].split("/")
        self.assertEqual(len(liens), 3, "Should be linked to 3 BD TOPO buildings")
        self.assertIn(
            bdtopo_cleabs_1, liens, "Should be linked to first BD TOPO building"
        )
        self.assertIn(
            bdtopo_cleabs_2, liens, "Should be linked to second BD TOPO building"
        )
        self.assertIn(
            bdtopo_cleabs_3, liens, "Should be linked to third BD TOPO building"
        )

    def test_partial_matching(self):
        """Test semantic pairing when only some ext_ids exist (partial matching filters to existing only)."""

        # Create only one BD TOPO building
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Create an RNB building with three ext_ids, but only one exists
        rnb_identifiant = "RNB_TEST_PARTIAL"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_identifiant,
            bdtopo_ids=[
                bdtopo_cleabs,  # This one exists
                "NONEXISTENT_CLEABS_1",  # This one doesn't exist
                "NONEXISTENT_CLEABS_2",  # This one doesn't exist
            ],
        )

        # Run the pairing process
        run_pairing_after_rnb_update()

        # Check the pairing result
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (rnb_identifiant,),
                )

        # Assertions - should still pair to the one that exists
        self.assertEqual(len(results), 1, "Expected exactly one RNB building")
        result = results[0]
        self.assertEqual(result["identifiant_rnb"], rnb_identifiant)
        self.assertEqual(
            result["liens_vers_batiment"],
            bdtopo_cleabs,
            "Should be linked only to the existing BD TOPO building",
        )
        self.assertEqual(
            result["traitement"], "Croisement sémantique", "Pairing should be semantic"
        )

    def test_multiple_rnb_to_same_bdtopo(self):
        """Test that multiple RNB buildings can point to the same BD TOPO building (N → 1)."""

        # Create one BD TOPO building
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Create two RNB buildings, both with the same BD TOPO cleabs in ext_ids
        # RNB 1: ~5x5m in the SW corner of the bdtopo footprint
        rnb_identifiant_1 = "RNB_TEST_N_TO_1_A"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000045],
                        [BASE_LON, BASE_LAT + 0.000045],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_identifiant_1,
            bdtopo_ids=[bdtopo_cleabs],
        )

        # RNB 2: ~5x5m in the NE corner of the bdtopo footprint
        rnb_identifiant_2 = "RNB_TEST_N_TO_1_B"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000067, BASE_LAT + 0.000045],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000045],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000045],
                    ]
                ],
            },
            identifiant_rnb=rnb_identifiant_2,
            bdtopo_ids=[bdtopo_cleabs],
        )

        # Run the pairing process
        run_pairing_after_rnb_update()

        # Check that both RNB buildings paired to the same BD TOPO building
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb IN (%s, %s)
                    ORDER BY identifiant_rnb
                    """,
                    (rnb_identifiant_1, rnb_identifiant_2),
                )

        # Assertions
        self.assertEqual(len(results), 2, "Expected two RNB buildings")

        # Check first RNB building
        result_1 = results[0]
        self.assertEqual(result_1["identifiant_rnb"], rnb_identifiant_1)
        self.assertEqual(
            result_1["liens_vers_batiment"],
            bdtopo_cleabs,
            "First RNB should be linked to BD TOPO",
        )
        self.assertEqual(
            result_1["traitement"],
            "Croisement sémantique",
            "First pairing should be semantic",
        )

        # Check second RNB building
        result_2 = results[1]
        self.assertEqual(result_2["identifiant_rnb"], rnb_identifiant_2)
        self.assertEqual(
            result_2["liens_vers_batiment"],
            bdtopo_cleabs,
            "Second RNB should be linked to same BD TOPO",
        )
        self.assertEqual(
            result_2["traitement"],
            "Croisement sémantique",
            "Second pairing should be semantic",
        )

    def test_empty_ext_ids(self):
        """Test that no semantic pairing occurs when ext_ids array is empty."""

        # Create a BD TOPO building
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Create an RNB building with empty ext_ids array (same geometry)
        rnb_identifiant = "RNB_TEST_EMPTY_EXT_IDS"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_identifiant,
            bdtopo_ids=[],  # Empty array
        )

        # Run the pairing process
        run_pairing_after_rnb_update()

        # Check that the RNB building was processed but not semantically paired
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (rnb_identifiant,),
                )

        # Assertions
        self.assertEqual(len(results), 1, "Expected exactly one RNB building")
        result = results[0]
        self.assertEqual(result["identifiant_rnb"], rnb_identifiant)

        # The key assertion: semantic pairing should NOT occur
        self.assertNotEqual(
            result["traitement"],
            "Croisement sémantique",
            "Pairing should NOT be semantic with empty ext_ids",
        )

        # Building may still be paired through geometric methods
        # In this case, the geometries match perfectly, so it pairs as "Batiments semblables"
        self.assertIsNotNone(
            result["liens_vers_batiment"],
            "Building may be paired through geometric methods",
        )
