import unittest
from db import dictfetchall, setup_db, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


# WGS84 coordinates near Orléans (dep 45)
# At lat ~47.9: 10m east ≈ +0.000134° lon, 10m north ≈ +0.000090° lat
BASE_LON = 1.900000
BASE_LAT = 47.900000

# Standard ~10x10m house polygon (5 points)
HOUSE_5PT = {
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

# Same shape, 6 points (extra midpoint on south edge)
HOUSE_6PT = {
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
}

# Same shape, 7 points (extra midpoints on south and north edges)
HOUSE_7PT = {
    "type": "Polygon",
    "coordinates": [
        [
            [BASE_LON, BASE_LAT],
            [BASE_LON + 0.000067, BASE_LAT],
            [BASE_LON + 0.000134, BASE_LAT],
            [BASE_LON + 0.000134, BASE_LAT + 0.000090],
            [BASE_LON + 0.000067, BASE_LAT + 0.000090],
            [BASE_LON, BASE_LAT + 0.000090],
            [BASE_LON, BASE_LAT],
        ]
    ],
}


class TestSimilarNbptDiff(unittest.TestCase):

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

    def _assert_not_paired_as_nbpt_diff(self, identifiant_rnb):
        """Assert a building was NOT paired as 'Batiments semblables nbpt diff'."""
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
                "Batiments semblables nbpt diff",
                "Should NOT be paired as 'Batiments semblables nbpt diff'",
            )

    def test_basic_match_extra_midpoint(self):
        """BD TOPO 5pts, RNB 6pts (midpoint on edge), same shape → pairs as nbpt diff."""

        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        rnb_id = "RNB_NBPT_BASIC"
        create_rnb_building(polygon_geojson=HOUSE_6PT, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments semblables nbpt diff")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    def test_more_points_on_bdtopo(self):
        """BD TOPO has more points than RNB → should still pair as nbpt diff."""

        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_7PT)

        rnb_id = "RNB_NBPT_MORE_ON_BDTOPO"
        create_rnb_building(polygon_geojson=HOUSE_5PT, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments semblables nbpt diff")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    def test_within_tolerance(self):
        """Different NPoints, area/perimeter within tolerance → pairs as nbpt diff."""

        # BD TOPO: 5pts ~10x10m
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB: 6pts ~10.2x9.8m — slightly different but within 3 m²/3 m tolerance
        rnb_id = "RNB_NBPT_TOLERANCE"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000069, BASE_LAT],
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
        self.assertEqual(result["traitement"], "Batiments semblables nbpt diff")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    def test_area_too_different(self):
        """Different NPoints but area diff >= 3 m² → should NOT pair as nbpt diff."""

        # BD TOPO: 5pts ~10x10m = ~100 m²
        create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB: 6pts ~10x20m = ~200 m² (area diff >> 3)
        rnb_id = "RNB_NBPT_AREA_DIFF"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT],
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

        self._assert_not_paired_as_nbpt_diff(rnb_id)

    def test_perimeter_too_different(self):
        """Different NPoints but perimeter diff >= 3 m → should NOT pair as nbpt diff."""

        # BD TOPO: 5pts ~10x10m → perimeter ≈ 40 m
        create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB: 6pts ~20x5m → area ≈ 100 m² (same), perimeter ≈ 50 m (diff ≈ 10)
        rnb_id = "RNB_NBPT_PERIM_DIFF"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT],
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

        self._assert_not_paired_as_nbpt_diff(rnb_id)

    def test_intersection_too_small(self):
        """Different NPoints, intersection <= 3 m² → should NOT pair as nbpt diff."""

        # BD TOPO: 5pts ~10x10m at base
        create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB: 6pts ~10x10m shifted ~9.7m east → overlap ≈ 0.3m * 10m = 3 m²
        # floor(3) = 3 and condition is > 3, so it should NOT match
        rnb_id = "RNB_NBPT_SMALL_INTERSECT"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000130, BASE_LAT],
                        [BASE_LON + 0.000197, BASE_LAT],
                        [BASE_LON + 0.000264, BASE_LAT],
                        [BASE_LON + 0.000264, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000130, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000130, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_nbpt_diff(rnb_id)

    def test_no_intersection(self):
        """Different NPoints, disjoint buildings → should NOT pair."""

        # BD TOPO at base
        create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB ~500m away, 6 points
        rnb_id = "RNB_NBPT_DISJOINT"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.006710, BASE_LAT],
                        [BASE_LON + 0.006777, BASE_LAT],
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

        self._assert_not_paired_as_nbpt_diff(rnb_id)

    def test_already_paired_by_semantic(self):
        """Building paired semantically should NOT appear as nbpt diff."""

        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB with ext_ids and different NPoints → semantic pairing takes priority
        rnb_id = "RNB_NBPT_SEMANTIC_FIRST"
        create_rnb_building(
            polygon_geojson=HOUSE_6PT,
            identifiant_rnb=rnb_id,
            bdtopo_ids=[bdtopo_cleabs],
        )

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(
            result["traitement"],
            "Croisement sémantique",
            "Should be paired semantically, not as nbpt diff",
        )

    def test_multiple_bdtopo_matches(self):
        """One RNB matches two BD TOPO buildings → liens concatenated with /."""

        # Two BD TOPO buildings at same location, both 5 points
        bdtopo_cleabs_1 = create_bdtopo_building(polygon_geojson=HOUSE_5PT)
        bdtopo_cleabs_2 = create_bdtopo_building(polygon_geojson=HOUSE_5PT)

        # RNB: 6 points, same location
        rnb_id = "RNB_NBPT_MULTI"
        create_rnb_building(polygon_geojson=HOUSE_6PT, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments semblables nbpt diff")
        liens = set(result["liens_vers_batiment"].split("/"))
        self.assertEqual(liens, {bdtopo_cleabs_1, bdtopo_cleabs_2})
