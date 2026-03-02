import unittest
from db import dictfetchall, setup_db, get_connection, get_cursor
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


class TestSimilar(unittest.TestCase):

    def setUp(self):
        """Reset database before each test to ensure clean state."""
        setup_db()

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

    def _assert_not_paired_as_similar(self, identifiant_rnb):
        """Assert a building was NOT paired as 'Batiments semblables'.
        It may be in traites with a different traitement, or still in restant."""
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                traites = dictfetchall(
                    cursor,
                    """
                    SELECT traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (identifiant_rnb,),
                )
        if len(traites) == 1:
            self.assertNotEqual(
                traites[0]["traitement"],
                "Batiments semblables",
                "Should NOT be paired as 'Batiments semblables'",
            )
        # If not in traites at all, it stayed in restant — also proves it wasn't paired as similar

    def test_identical_geometries(self):
        """Test that identical geometries (no ext_ids) pair as 'Batiments semblables'."""

        # Same ~10x10m house for both
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        rnb_id = "RNB_SIMILAR_IDENTICAL"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments semblables")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    def test_within_tolerance(self):
        """Test that buildings with area/perimeter differences < 3 still pair."""

        # BD TOPO: ~10x10m house
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB: ~10.2x9.8m → area ≈ 99.96 m², perimeter ≈ 40 m — within tolerance
        rnb_id = "RNB_SIMILAR_TOLERANCE"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000137, BASE_LAT],
                        [BASE_LON + 0.000137, BASE_LAT + 0.000088],
                        [BASE_LON, BASE_LAT + 0.000088],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments semblables")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    def test_area_too_different(self):
        """Test that buildings with area difference >= 3 m² do NOT pair as similar."""

        # BD TOPO: ~10x10m = ~100 m²
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB: ~10x20m = ~200 m² (difference ≈ 100 m², well above threshold)
        rnb_id = "RNB_SIMILAR_AREA_DIFF"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000180],
                        [BASE_LON, BASE_LAT + 0.000180],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_similar(rnb_id)

    def test_perimeter_too_different(self):
        """Test that buildings with perimeter difference >= 3 m do NOT pair as similar."""

        # BD TOPO: ~10x10m → perimeter ≈ 40 m
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB: ~20x5m → area ≈ 100 m² (same), perimeter ≈ 50 m (diff ≈ 10 m, above threshold)
        rnb_id = "RNB_SIMILAR_PERIM_DIFF"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000268, BASE_LAT],
                        [BASE_LON + 0.000268, BASE_LAT + 0.000045],
                        [BASE_LON, BASE_LAT + 0.000045],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_similar(rnb_id)

    def test_different_npoints_not_matched(self):
        """Test that buildings with different vertex counts are NOT matched as 'Batiments semblables'
        (they should fall through to 'Batiments semblables nbpt diff')."""

        # BD TOPO: ~10x10m house → 5 points
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB: same shape but 6 points (extra midpoint on south edge)
        rnb_id = "RNB_SIMILAR_NPOINTS_DIFF"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertNotEqual(
            result["traitement"],
            "Batiments semblables",
            "Should NOT be 'Batiments semblables' with different NPoints",
        )
        self.assertEqual(
            result["traitement"],
            "Batiments semblables nbpt diff",
            "Should fall through to 'nbpt diff' step",
        )
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    def test_no_intersection(self):
        """Test that disjoint buildings do not pair."""

        # BD TOPO at base location
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB ~500m away (same shape, same NPoints)
        rnb_id = "RNB_SIMILAR_DISJOINT"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.006710, BASE_LAT],
                        [BASE_LON + 0.006844, BASE_LAT],
                        [BASE_LON + 0.006844, BASE_LAT + 0.000090],
                        [BASE_LON + 0.006710, BASE_LAT + 0.000090],
                        [BASE_LON + 0.006710, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                traites = dictfetchall(
                    cursor,
                    """
                    SELECT traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (rnb_id,),
                )
        self.assertEqual(len(traites), 0, "Disjoint building should not be paired at all")

    def test_already_paired_by_semantic(self):
        """Test that a building paired semantically does NOT appear again as 'Batiments semblables'."""

        # BD TOPO building
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB building with ext_ids → will be consumed by semantic pairing
        rnb_id = "RNB_SIMILAR_SEMANTIC_FIRST"
        create_rnb_building(
            polygon_geojson=HOUSE_10x10,
            identifiant_rnb=rnb_id,
            bdtopo_ids=[bdtopo_cleabs],
        )

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(
            result["traitement"],
            "Croisement sémantique",
            "Should be paired semantically, not as similar",
        )

    def test_intersection_at_threshold(self):
        """Test that partial overlap with different area/perimeter does NOT match as similar."""

        # BD TOPO: ~10x10m house at base
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB: ~3x10m rectangle overlapping ~1m with the BD TOPO building
        # Starts ~9m east (lon + 0.000121), width ~3m (lon + 0.000040)
        # The overlap is ~1x10m ≈ 10 m² → intersection > 3, but area diff >> 3
        rnb_id = "RNB_SIMILAR_INTERSECT_THRESHOLD"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000121, BASE_LAT],
                        [BASE_LON + 0.000161, BASE_LAT],
                        [BASE_LON + 0.000161, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000121, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000121, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_similar(rnb_id)
