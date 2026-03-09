import unittest
from db import dictfetchall, setup_db, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


# WGS84 coordinates near Orléans (dep 45)
# At lat ~47.9: 10m east ≈ +0.000134° lon, 10m north ≈ +0.000090° lat
# 1m east ≈ +0.0000134° lon, 1m north ≈ +0.0000090° lat
BASE_LON = 1.900000
BASE_LAT = 47.900000

# Standard ~10x10m house polygon (~100 m²)
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

# Small ~4x4m balcony polygon (~16 m²), positioned just east of the house
# Overlapping ~0.5m on the east edge of the house
# The balcony spans from lon+0.000127 to lon+0.000181 (4m wide)
# so overlap with house (which ends at lon+0.000134) is ~0.5m * 4m = ~2 m²
BALCONY_4x4_OVERLAPPING = {
    "type": "Polygon",
    "coordinates": [
        [
            [BASE_LON + 0.000127, BASE_LAT + 0.000027],
            [BASE_LON + 0.000181, BASE_LAT + 0.000027],
            [BASE_LON + 0.000181, BASE_LAT + 0.000063],
            [BASE_LON + 0.000127, BASE_LAT + 0.000063],
            [BASE_LON + 0.000127, BASE_LAT + 0.000027],
        ]
    ],
}


class TestBalcony(unittest.TestCase):

    def setUp(self):
        """Reset database before each test to ensure clean state."""
        setup_db()

    def _get_bdtopo_result(self, cleabs):
        """Helper to query the BD TOPO processed result for a given cleabs."""
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT cleabs, traitement, identifiant_rnb
                    FROM processus_divers.rnb_batiments_bduni_traites_creation
                    WHERE cleabs = %s
                    """,
                    (cleabs,),
                )
        return results

    def _get_pairing_result(self, identifiant_rnb):
        """Helper to query the pairing result for a given RNB building."""
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                results = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (identifiant_rnb,),
                )
        self.assertEqual(len(results), 1, "Expected exactly one RNB building in traites")
        return results[0]

    def test_small_bdtopo_detected_as_balcony(self):
        """Small BD TOPO (<25 m²) barely overlapping a larger RNB building (<3 m² intersection)
        should be detected as balcony."""

        # Create the main RNB house (~100 m²)
        rnb_id = "RNB_BALCONY_HAPPY"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        # Create a small BD TOPO balcony (~16 m²) barely overlapping east edge
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=BALCONY_4x4_OVERLAPPING)

        run_pairing_after_rnb_update()

        # The RNB building should be treated as having a balcony
        result = self._get_pairing_result(rnb_id)
        self.assertEqual(
            result["traitement"],
            "Batiments BDTOPO balcon",
            "RNB building should be marked with balcony treatment",
        )
        self.assertEqual(
            result["liens_vers_batiment"],
            bdtopo_cleabs,
            "RNB building should be linked to the balcony BD TOPO",
        )

        # The BD TOPO balcony should be moved to processed table
        bdtopo_results = self._get_bdtopo_result(bdtopo_cleabs)
        self.assertEqual(
            len(bdtopo_results), 1, "BD TOPO balcony should be in processed table"
        )
        self.assertEqual(
            bdtopo_results[0]["traitement"],
            "Batiments BDTOPO balcon",
            "BD TOPO should be marked as balcony",
        )

    def test_bdtopo_exactly_25m2_not_balcony(self):
        """BD TOPO structure with area exactly 25 m² should NOT be detected as balcony
        (condition is strict < 25)."""

        # Create the main RNB house (~100 m²)
        rnb_id = "RNB_BALCONY_EXACT25"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        # Create a BD TOPO structure ~5x5m = ~25 m², barely overlapping the house
        # Positioned so it overlaps ~0.5m on the east edge
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000127, BASE_LAT + 0.000023],
                        [BASE_LON + 0.000194, BASE_LAT + 0.000023],
                        [BASE_LON + 0.000194, BASE_LAT + 0.000068],
                        [BASE_LON + 0.000127, BASE_LAT + 0.000068],
                        [BASE_LON + 0.000127, BASE_LAT + 0.000023],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        # The BD TOPO should NOT be in the processed table as a balcony
        bdtopo_results = self._get_bdtopo_result(bdtopo_cleabs)
        for r in bdtopo_results:
            self.assertNotEqual(
                r["traitement"],
                "Batiments BDTOPO balcon",
                "25 m² BD TOPO should NOT be detected as balcony",
            )

    def test_large_bdtopo_not_balcony(self):
        """BD TOPO structure larger than 25 m² should NOT be detected as balcony."""

        # Create the main RNB house (~100 m²)
        rnb_id = "RNB_BALCONY_LARGE"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        # Create a BD TOPO ~8x8m = ~64 m² (well above 25 m²), overlapping the house
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000027, BASE_LAT + 0.000009],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000009],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000081],
                        [BASE_LON + 0.000027, BASE_LAT + 0.000081],
                        [BASE_LON + 0.000027, BASE_LAT + 0.000009],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        # The BD TOPO should NOT be in the processed table as a balcony
        bdtopo_results = self._get_bdtopo_result(bdtopo_cleabs)
        for r in bdtopo_results:
            self.assertNotEqual(
                r["traitement"],
                "Batiments BDTOPO balcon",
                "Large BD TOPO (>25 m²) should NOT be detected as balcony",
            )

    def test_no_intersection_not_balcony(self):
        """Small BD TOPO (<25 m²) that does NOT intersect any RNB building
        should NOT be detected as balcony."""

        # Create the main RNB house
        rnb_id = "RNB_BALCONY_NOINTERSECT"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        # Create a small BD TOPO ~4x4m but 30m away from the house (no intersection)
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000536, BASE_LAT],
                        [BASE_LON + 0.000590, BASE_LAT],
                        [BASE_LON + 0.000590, BASE_LAT + 0.000036],
                        [BASE_LON + 0.000536, BASE_LAT + 0.000036],
                        [BASE_LON + 0.000536, BASE_LAT],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        # The BD TOPO should NOT be in the processed table as a balcony
        bdtopo_results = self._get_bdtopo_result(bdtopo_cleabs)
        for r in bdtopo_results:
            self.assertNotEqual(
                r["traitement"],
                "Batiments BDTOPO balcon",
                "Non-intersecting BD TOPO should NOT be detected as balcony",
            )

    def test_bdtopo_larger_than_rnb_not_balcony(self):
        """Small BD TOPO (<25 m²) that is larger than the RNB building
        should NOT be detected as balcony (bdtopo area must be < rnb area)."""

        # Create a tiny RNB building ~3x3m = ~9 m²
        rnb_id = "RNB_BALCONY_TINY_RNB"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000040, BASE_LAT],
                        [BASE_LON + 0.000040, BASE_LAT + 0.000027],
                        [BASE_LON, BASE_LAT + 0.000027],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        # Create a BD TOPO ~4x4m = ~16 m² (larger than the RNB at ~9 m²)
        # Positioned to barely overlap the RNB building
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000033, BASE_LAT],
                        [BASE_LON + 0.000087, BASE_LAT],
                        [BASE_LON + 0.000087, BASE_LAT + 0.000036],
                        [BASE_LON + 0.000033, BASE_LAT + 0.000036],
                        [BASE_LON + 0.000033, BASE_LAT],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        # The BD TOPO should NOT be marked as balcony
        bdtopo_results = self._get_bdtopo_result(bdtopo_cleabs)
        for r in bdtopo_results:
            self.assertNotEqual(
                r["traitement"],
                "Batiments BDTOPO balcon",
                "BD TOPO larger than RNB should NOT be detected as balcony",
            )

    def test_intersection_too_large_not_balcony(self):
        """Small BD TOPO (<25 m²) with intersection area >= 3 m²
        should NOT be detected as balcony."""

        # Create the main RNB house (~100 m²)
        rnb_id = "RNB_BALCONY_BIGOVERLAP"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        # Create a small BD TOPO ~4x4m (~16 m²) that is mostly inside the house
        # Starts inside the house at lon+0.000100, so overlap is ~2.5m * 4m = ~10 m²
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000067, BASE_LAT + 0.000027],
                        [BASE_LON + 0.000121, BASE_LAT + 0.000027],
                        [BASE_LON + 0.000121, BASE_LAT + 0.000063],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000063],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000027],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        # The BD TOPO should NOT be marked as balcony (intersection too large)
        bdtopo_results = self._get_bdtopo_result(bdtopo_cleabs)
        for r in bdtopo_results:
            self.assertNotEqual(
                r["traitement"],
                "Batiments BDTOPO balcon",
                "BD TOPO with large intersection (>= 3 m²) should NOT be detected as balcony",
            )

    def test_balcony_matched_against_already_treated_rnb(self):
        """A balcony should be detected even if the RNB building was already treated
        by a previous step (semantic pairing). The SQL uses UNION of restant and traites."""

        # Create a BD TOPO building that will match semantically with the RNB
        bdtopo_main = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Create the RNB building with ext_ids so it gets paired semantically first
        rnb_id = "RNB_BALCONY_TREATED"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
            bdtopo_ids=[bdtopo_main],
        )

        # Create a small balcony barely overlapping the house
        bdtopo_balcony = create_bdtopo_building(
            polygon_geojson=BALCONY_4x4_OVERLAPPING
        )

        run_pairing_after_rnb_update()

        # The RNB building should first be treated as semantic
        # But the balcony step looks at UNION of restant and traites,
        # so it should still detect the balcony against the already-treated RNB
        bdtopo_results = self._get_bdtopo_result(bdtopo_balcony)
        self.assertEqual(
            len(bdtopo_results), 1, "BD TOPO balcony should be in processed table"
        )
        self.assertEqual(
            bdtopo_results[0]["traitement"],
            "Batiments BDTOPO balcon",
            "Balcony should be detected even against already-treated RNB",
        )

    def test_multiple_balconies_for_one_rnb(self):
        """Multiple small BD TOPO structures around one RNB building
        should all be detected as balconies and linked."""

        # Create the main RNB house (~100 m²)
        rnb_id = "RNB_BALCONY_MULTI"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        # Balcony 1: east side of the house
        bdtopo_balcony_1 = create_bdtopo_building(
            polygon_geojson=BALCONY_4x4_OVERLAPPING
        )

        # Balcony 2: west side of the house (~4x4m barely overlapping west edge)
        bdtopo_balcony_2 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON - 0.000047, BASE_LAT + 0.000027],
                        [BASE_LON + 0.000007, BASE_LAT + 0.000027],
                        [BASE_LON + 0.000007, BASE_LAT + 0.000063],
                        [BASE_LON - 0.000047, BASE_LAT + 0.000063],
                        [BASE_LON - 0.000047, BASE_LAT + 0.000027],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        # The RNB building should be linked to both balconies
        result = self._get_pairing_result(rnb_id)
        self.assertEqual(
            result["traitement"],
            "Batiments BDTOPO balcon",
            "RNB building should be marked with balcony treatment",
        )
        liens = set(result["liens_vers_batiment"].split("/"))
        self.assertEqual(
            len(liens), 2, "Should be linked to 2 balcony BD TOPO buildings"
        )
        self.assertIn(bdtopo_balcony_1, liens, "Should include first balcony")
        self.assertIn(bdtopo_balcony_2, liens, "Should include second balcony")
