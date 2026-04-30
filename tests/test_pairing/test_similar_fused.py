import unittest
from db import dictfetchall, setup_db, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


# WGS84 coordinates near Orléans (dep 45)
# At lat ~47.9: 10m east ≈ +0.000134° lon, 10m north ≈ +0.000090° lat
BASE_LON = 1.900000
BASE_LAT = 47.900000

# Standard ~10x10m house polygon (BD TOPO building)
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

# Left half: ~5x10m (west half of 10x10m house)
RNB_LEFT_HALF = {
    "type": "Polygon",
    "coordinates": [
        [
            [BASE_LON, BASE_LAT],
            [BASE_LON + 0.000067, BASE_LAT],
            [BASE_LON + 0.000067, BASE_LAT + 0.000090],
            [BASE_LON, BASE_LAT + 0.000090],
            [BASE_LON, BASE_LAT],
        ]
    ],
}

# Right half: ~5x10m (east half of 10x10m house)
RNB_RIGHT_HALF = {
    "type": "Polygon",
    "coordinates": [
        [
            [BASE_LON + 0.000067, BASE_LAT],
            [BASE_LON + 0.000134, BASE_LAT],
            [BASE_LON + 0.000134, BASE_LAT + 0.000090],
            [BASE_LON + 0.000067, BASE_LAT + 0.000090],
            [BASE_LON + 0.000067, BASE_LAT],
        ]
    ],
}


class TestSimilarFused(unittest.TestCase):

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
        self.assertEqual(
            len(results), 1, "Expected exactly one RNB building in traites"
        )
        return results[0]

    def _assert_not_paired_as_fused(self, identifiant_rnb):
        """Assert a building was NOT paired as 'Batiments RNB dissous semblables'.
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
                "Batiments RNB dissous semblables",
                "Should NOT be paired as 'Batiments RNB dissous semblables'",
            )
        # If not in traites at all, it stayed in restant — also proves it wasn't paired as fused

    def test_two_adjacent_rnb_fuse_to_match_bdtopo(self):
        """Two adjacent RNB buildings (left+right halves) fuse to match a single BD TOPO building.
        Each half is ~5x10m (area ~50 m²), but their union is ~10x10m (area ~100 m²)
        which matches the BD TOPO building."""

        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        rnb_id_left = "RNB_FUSED_LEFT"
        create_rnb_building(polygon_geojson=RNB_LEFT_HALF, identifiant_rnb=rnb_id_left)

        rnb_id_right = "RNB_FUSED_RIGHT"
        create_rnb_building(
            polygon_geojson=RNB_RIGHT_HALF, identifiant_rnb=rnb_id_right
        )

        run_pairing_after_rnb_update()

        result_left = self._get_pairing_result(rnb_id_left)
        self.assertEqual(result_left["traitement"], "Batiments RNB dissous semblables")
        self.assertEqual(result_left["liens_vers_batiment"], bdtopo_cleabs)

        result_right = self._get_pairing_result(rnb_id_right)
        self.assertEqual(result_right["traitement"], "Batiments RNB dissous semblables")
        self.assertEqual(result_right["liens_vers_batiment"], bdtopo_cleabs)

    def test_three_adjacent_rnb_fuse_to_match_bdtopo(self):
        """Three adjacent RNB buildings fuse to match a single BD TOPO building.
        BD TOPO is ~15x10m. Three RNB buildings are each ~5x10m side by side."""

        # BD TOPO: ~15x10m warehouse
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000201, BASE_LAT],
                        [BASE_LON + 0.000201, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            }
        )

        # RNB 1: left third ~5x10m
        rnb_id_1 = "RNB_FUSED_THIRD_1"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_1,
        )

        # RNB 2: middle third ~5x10m
        rnb_id_2 = "RNB_FUSED_THIRD_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000067, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000067, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        # RNB 3: right third ~5x10m
        rnb_id_3 = "RNB_FUSED_THIRD_3"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000134, BASE_LAT],
                        [BASE_LON + 0.000201, BASE_LAT],
                        [BASE_LON + 0.000201, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000134, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_3,
        )

        run_pairing_after_rnb_update()

        for rnb_id in [rnb_id_1, rnb_id_2, rnb_id_3]:
            result = self._get_pairing_result(rnb_id)
            self.assertEqual(
                result["traitement"],
                "Batiments RNB dissous semblables",
                f"{rnb_id} should be paired as fused similar",
            )
            self.assertEqual(
                result["liens_vers_batiment"],
                bdtopo_cleabs,
                f"{rnb_id} should be linked to the BD TOPO building",
            )

    def test_non_adjacent_rnb_not_fused(self):
        """Two RNB buildings that do NOT touch each other should NOT be fused.
        Both are ~5x10m (area ~50 m²) and both sit inside a ~20x10m BD TOPO (area ~200 m²).
        Since they don't intersect, ST_Union won't merge them into one component,
        and neither individually nor combined do they match the BD TOPO (area diff >> 3 m²).
        """

        # BD TOPO: ~20x10m — wide enough to contain both RNB buildings
        create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000268, BASE_LAT],
                        [BASE_LON + 0.000268, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            }
        )

        # RNB 1: ~5x10m at base location
        rnb_id_1 = "RNB_FUSED_NONADJ_1"
        create_rnb_building(polygon_geojson=RNB_LEFT_HALF, identifiant_rnb=rnb_id_1)

        # RNB 2: ~5x10m on the right side of BD TOPO, sharing its right edge
        # RNB 1 ends at BASE_LON + 0.000067, RNB 2 starts at BASE_LON + 0.000201 (~10m gap)
        rnb_id_2 = "RNB_FUSED_NONADJ_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000201, BASE_LAT],
                        [BASE_LON + 0.000268, BASE_LAT],
                        [BASE_LON + 0.000268, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000201, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000201, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_fused(rnb_id_1)
        self._assert_not_paired_as_fused(rnb_id_2)

    def test_fused_area_too_different(self):
        """Two adjacent RNB buildings fuse, but the fused area is too different from BD TOPO.
        BD TOPO is ~10x10m (area ~100 m²).
        Two RNB buildings are ~5x20m each (area ~100 m² each), adjacent,
        fused area ~10x20m = ~200 m² => area diff ~100 m² >> 3."""

        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB 1: ~5x20m (left half, tall)
        rnb_id_1 = "RNB_FUSED_AREA_DIFF_1"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000180],
                        [BASE_LON, BASE_LAT + 0.000180],
                        [BASE_LON, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_1,
        )

        # RNB 2: ~5x20m (right half, tall)
        rnb_id_2 = "RNB_FUSED_AREA_DIFF_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000067, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT],
                        [BASE_LON + 0.000134, BASE_LAT + 0.000180],
                        [BASE_LON + 0.000067, BASE_LAT + 0.000180],
                        [BASE_LON + 0.000067, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_fused(rnb_id_1)
        self._assert_not_paired_as_fused(rnb_id_2)

        """Two adjacent RNB buildings fuse, but the fused perimeter is too different from BD TOPO.
        BD TOPO is ~10x10m (perimeter ~40 m).
        Two RNB buildings are ~10x5m (top+bottom halves), adjacent horizontally,
        fused shape is ~20x5m => area ~100 m² (same), perimeter ~50 m (diff ~10 m >> 3)."""

        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # RNB 1: ~10x5m bottom half
        rnb_id_1 = "RNB_FUSED_PERIM_DIFF_1"
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
            identifiant_rnb=rnb_id_1,
        )

        # RNB 2: ~10x5m top half (adjacent on the north edge of RNB 1)
        rnb_id_2 = "RNB_FUSED_PERIM_DIFF_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON, BASE_LAT + 0.000045],
                        [BASE_LON + 0.000268, BASE_LAT + 0.000045],
                        [BASE_LON + 0.000268, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT + 0.000090],
                        [BASE_LON, BASE_LAT + 0.000045],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_fused(rnb_id_1)
        self._assert_not_paired_as_fused(rnb_id_2)

    def test_fused_intersection_too_small(self):
        """Two adjacent RNB buildings fuse, but the fused shape barely intersects BD TOPO
        (intersection area <= 3 m²) so it should NOT match."""

        # BD TOPO: ~10x10m at base
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Two adjacent RNB buildings forming a ~10x10m shape,
        # but shifted far east so only a sliver overlaps with BD TOPO
        # Shift by ~9.7m east (0.000130 lon) => overlap ~0.3m * 10m = 3 m²
        # floor(3) = 3, condition is > 3, so should NOT match

        # RNB 1: left half of shifted pair
        rnb_id_1 = "RNB_FUSED_SMALL_INT_1"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000130, BASE_LAT],
                        [BASE_LON + 0.000197, BASE_LAT],
                        [BASE_LON + 0.000197, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000130, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000130, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_1,
        )

        # RNB 2: right half of shifted pair
        rnb_id_2 = "RNB_FUSED_SMALL_INT_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.000197, BASE_LAT],
                        [BASE_LON + 0.000264, BASE_LAT],
                        [BASE_LON + 0.000264, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000197, BASE_LAT + 0.000090],
                        [BASE_LON + 0.000197, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_fused(rnb_id_1)
        self._assert_not_paired_as_fused(rnb_id_2)

    def test_already_paired_by_earlier_step(self):
        """Buildings paired by semantic pairing should NOT appear as fused similar.
        An RNB building with ext_ids referencing a BD TOPO building gets consumed
        in the semantic step and should not reach the fusion step."""

        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        rnb_id_left = "RNB_FUSED_SEMANTIC_1"
        create_rnb_building(
            polygon_geojson=RNB_LEFT_HALF,
            identifiant_rnb=rnb_id_left,
            bdtopo_ids=[bdtopo_cleabs],
        )

        rnb_id_right = "RNB_FUSED_SEMANTIC_2"
        create_rnb_building(
            polygon_geojson=RNB_RIGHT_HALF,
            identifiant_rnb=rnb_id_right,
            bdtopo_ids=[bdtopo_cleabs],
        )

        run_pairing_after_rnb_update()

        result_left = self._get_pairing_result(rnb_id_left)
        self.assertEqual(
            result_left["traitement"],
            "Croisement sémantique",
            "Should be paired semantically, not as fused similar",
        )

        result_right = self._get_pairing_result(rnb_id_right)
        self.assertEqual(
            result_right["traitement"],
            "Croisement sémantique",
            "Should be paired semantically, not as fused similar",
        )

    def test_multiple_bdtopo_matches(self):
        """Two adjacent RNB buildings fuse, and match two overlapping BD TOPO buildings.
        Both BD TOPO cleabs should appear in liens_vers_batiment separated by /."""

        # Two identical BD TOPO buildings at same location
        bdtopo_cleabs_1 = create_bdtopo_building(polygon_geojson=HOUSE_10x10)
        bdtopo_cleabs_2 = create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        rnb_id_left = "RNB_FUSED_MULTI_1"
        create_rnb_building(polygon_geojson=RNB_LEFT_HALF, identifiant_rnb=rnb_id_left)

        rnb_id_right = "RNB_FUSED_MULTI_2"
        create_rnb_building(
            polygon_geojson=RNB_RIGHT_HALF, identifiant_rnb=rnb_id_right
        )

        run_pairing_after_rnb_update()

        result_left = self._get_pairing_result(rnb_id_left)
        self.assertEqual(result_left["traitement"], "Batiments RNB dissous semblables")
        liens_left = set(result_left["liens_vers_batiment"].split("/"))
        self.assertEqual(liens_left, {bdtopo_cleabs_1, bdtopo_cleabs_2})

        result_right = self._get_pairing_result(rnb_id_right)
        self.assertEqual(result_right["traitement"], "Batiments RNB dissous semblables")
        liens_right = set(result_right["liens_vers_batiment"].split("/"))
        self.assertEqual(liens_right, {bdtopo_cleabs_1, bdtopo_cleabs_2})

    def test_no_intersection_with_bdtopo(self):
        """Two adjacent RNB buildings fuse but the fused shape is far from BD TOPO.
        No intersection at all → should NOT match."""

        # BD TOPO at base
        create_bdtopo_building(polygon_geojson=HOUSE_10x10)

        # Two adjacent RNB buildings ~500m away
        rnb_id_1 = "RNB_FUSED_DISJOINT_1"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.006710, BASE_LAT],
                        [BASE_LON + 0.006777, BASE_LAT],
                        [BASE_LON + 0.006777, BASE_LAT + 0.000090],
                        [BASE_LON + 0.006710, BASE_LAT + 0.000090],
                        [BASE_LON + 0.006710, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_1,
        )

        rnb_id_2 = "RNB_FUSED_DISJOINT_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [BASE_LON + 0.006777, BASE_LAT],
                        [BASE_LON + 0.006844, BASE_LAT],
                        [BASE_LON + 0.006844, BASE_LAT + 0.000090],
                        [BASE_LON + 0.006777, BASE_LAT + 0.000090],
                        [BASE_LON + 0.006777, BASE_LAT],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_fused(rnb_id_1)
        self._assert_not_paired_as_fused(rnb_id_2)
