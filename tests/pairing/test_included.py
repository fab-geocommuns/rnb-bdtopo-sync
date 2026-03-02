import unittest
from db import dictfetchall, setup_db, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


# WGS84 coordinates near Orleans (dep 45)
# At lat ~47.9: 10m east ~ +0.000134 lon, 10m north ~ +0.000090 lat
# So 1m east ~ +0.0000134 lon, 1m north ~ +0.0000090 lat
BASE_LON = 1.900000
BASE_LAT = 47.900000

# Conversion helpers (approximate at lat 47.9)
M_TO_LON = 0.0000134  # 1 meter in longitude degrees
M_TO_LAT = 0.0000090  # 1 meter in latitude degrees


def _rect(x0_m, y0_m, width_m, height_m):
    """Build a rectangular GeoJSON polygon from metric offsets relative to BASE_LON/BASE_LAT.

    Parameters are in meters:
      x0_m, y0_m  : SW corner offset from base
      width_m      : east-west extent
      height_m     : north-south extent
    """
    lon0 = BASE_LON + x0_m * M_TO_LON
    lat0 = BASE_LAT + y0_m * M_TO_LAT
    lon1 = lon0 + width_m * M_TO_LON
    lat1 = lat0 + height_m * M_TO_LAT
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [lon0, lat0],
                [lon1, lat0],
                [lon1, lat1],
                [lon0, lat1],
                [lon0, lat0],
            ]
        ],
    }


# Large BD TOPO warehouse: 30x30m at origin
WAREHOUSE_30x30 = _rect(0, 0, 30, 30)

# Small RNB house: 8x8m centered inside the warehouse (offset 11m from each edge)
HOUSE_8x8_CENTERED = _rect(11, 11, 8, 8)


class TestIncluded(unittest.TestCase):

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

    def _assert_not_paired_as_included(self, identifiant_rnb):
        """Assert a building was NOT paired as 'Batiments inclus'.
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
                "Batiments inclus",
                "Should NOT be paired as 'Batiments inclus'",
            )

    # -------------------------------------------------------------------------
    # Happy path: small RNB building well inside a large BD TOPO building
    # -------------------------------------------------------------------------

    def test_small_rnb_inside_large_bdtopo(self):
        """An 8x8m RNB building centered inside a 30x30m BD TOPO warehouse pairs as 'Batiments inclus'.

        The RNB building is 11m from each edge of the BD TOPO building, so the -0.5m buffer
        still leaves it well inside.
        """
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=WAREHOUSE_30x30)

        rnb_id = "RNB_INCLUDED_HAPPY"
        create_rnb_building(polygon_geojson=HOUSE_8x8_CENTERED, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments inclus")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    # -------------------------------------------------------------------------
    # Edge case: RNB building barely fits after -0.5m buffer
    # -------------------------------------------------------------------------

    def test_rnb_barely_fits_with_buffer(self):
        """RNB building 1m from each edge of BD TOPO. After -0.5m buffer it shrinks but
        still fits inside (0.5m clearance remains). Should pair as 'Batiments inclus'.

        BD TOPO: 20x20m at origin.
        RNB: 18x18m offset 1m from each edge => edges at 1m and 19m.
        After -0.5m buffer: RNB becomes ~17x17m at offsets 1.5m to 18.5m.
        BD TOPO spans 0 to 20m, so 1.5m and 18.5m are well inside.
        """
        bdtopo = _rect(0, 0, 20, 20)
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=bdtopo)

        rnb = _rect(1, 1, 18, 18)
        rnb_id = "RNB_INCLUDED_BARELY_FITS"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments inclus")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    # -------------------------------------------------------------------------
    # Edge case: RNB building looks contained but after -0.5m buffer it no longer fits
    # -------------------------------------------------------------------------

    def test_rnb_overhangs_too_much_for_buffer(self):
        """RNB building overhangs BD TOPO by ~1m on the east side. After -0.5m buffer,
        it still overhangs by 0.5m, so ST_Contains fails. Should NOT pair as 'Batiments inclus'.

        BD TOPO: 20x20m at origin (0 to 20m).
        RNB: 8x8m at x=13m => east edge at 21m, 1m overshoot.
        After -0.5m buffer: RNB becomes ~7x7m at 13.5m to 20.5m. Still outside.
        """
        # RNB overhangs ~1m on the east side. After -0.5m buffer, still 0.5m overhang.
        # BD TOPO: 20x20m. RNB: 8x8m starting at x=13m (ends at x=21m, 1m overhang).
        bdtopo = _rect(0, 0, 20, 20)
        create_bdtopo_building(polygon_geojson=bdtopo)

        rnb = _rect(13, 6, 8, 8)  # east edge at 21m, overhangs 1m
        rnb_id = "RNB_INCLUDED_BUFFER_FAIL"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_included(rnb_id)

    # -------------------------------------------------------------------------
    # Edge case: small overshoot saved by the -0.5m buffer
    # -------------------------------------------------------------------------

    def test_small_overshoot_saved_by_buffer(self):
        """RNB overhangs BD TOPO by ~0.3m. After -0.5m buffer, it shrinks enough
        to fit inside. Should pair as 'Batiments inclus'.

        BD TOPO: 20x20m at origin (0 to 20m).
        RNB: 10x10m starting at x=10.3m, y=5m => east edge at 20.3m, 0.3m overhang.
        After -0.5m buffer: RNB becomes ~9x9m at x=10.8m to 19.8m. Now inside BD TOPO.
        """
        bdtopo = _rect(0, 0, 20, 20)
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=bdtopo)

        rnb = _rect(10.3, 5, 10, 10)  # east edge at 20.3m, 0.3m overshoot
        rnb_id = "RNB_INCLUDED_OVERSHOOT_SAVED"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments inclus")
        self.assertEqual(result["liens_vers_batiment"], bdtopo_cleabs)

    # -------------------------------------------------------------------------
    # Negative: RNB building partially overlaps but is NOT contained
    # -------------------------------------------------------------------------

    def test_partial_overlap_not_contained(self):
        """RNB building overlaps BD TOPO on one side but extends well outside.
        Should NOT pair as 'Batiments inclus'.

        BD TOPO: 20x20m at origin. RNB: 10x10m starting at x=15m (half outside).
        """
        bdtopo = _rect(0, 0, 20, 20)
        create_bdtopo_building(polygon_geojson=bdtopo)

        rnb = _rect(15, 5, 10, 10)  # extends from x=15m to x=25m, 5m outside
        rnb_id = "RNB_INCLUDED_PARTIAL"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_included(rnb_id)

    # -------------------------------------------------------------------------
    # Negative: disjoint buildings
    # -------------------------------------------------------------------------

    def test_disjoint_buildings(self):
        """RNB building is far from BD TOPO. Should NOT pair as 'Batiments inclus'."""
        bdtopo = _rect(0, 0, 20, 20)
        create_bdtopo_building(polygon_geojson=bdtopo)

        rnb = _rect(50, 50, 8, 8)  # 50m away
        rnb_id = "RNB_INCLUDED_DISJOINT"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_included(rnb_id)

    # -------------------------------------------------------------------------
    # Multiple RNB buildings inside the same BD TOPO building
    # -------------------------------------------------------------------------

    def test_multiple_rnb_inside_same_bdtopo(self):
        """Two small RNB buildings inside a large BD TOPO warehouse.
        Both should pair as 'Batiments inclus' to the same BD TOPO building.
        """
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=WAREHOUSE_30x30)

        # RNB 1: 5x5m in the SW quadrant
        rnb_id_1 = "RNB_INCLUDED_MULTI_A"
        create_rnb_building(
            polygon_geojson=_rect(3, 3, 5, 5),
            identifiant_rnb=rnb_id_1,
        )

        # RNB 2: 5x5m in the NE quadrant
        rnb_id_2 = "RNB_INCLUDED_MULTI_B"
        create_rnb_building(
            polygon_geojson=_rect(22, 22, 5, 5),
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        result_1 = self._get_pairing_result(rnb_id_1)
        self.assertEqual(result_1["traitement"], "Batiments inclus")
        self.assertEqual(result_1["liens_vers_batiment"], bdtopo_cleabs)

        result_2 = self._get_pairing_result(rnb_id_2)
        self.assertEqual(result_2["traitement"], "Batiments inclus")
        self.assertEqual(result_2["liens_vers_batiment"], bdtopo_cleabs)

    # -------------------------------------------------------------------------
    # One RNB building contained in multiple BD TOPO buildings (overlapping BD TOPO)
    # -------------------------------------------------------------------------

    def test_rnb_inside_multiple_overlapping_bdtopo(self):
        """RNB building is inside two overlapping BD TOPO buildings.
        liens_vers_batiment should contain both cleabs separated by /.
        """
        # Two overlapping BD TOPO buildings (both 20x20m, offset by 5m)
        bdtopo_1 = _rect(0, 0, 20, 20)
        bdtopo_cleabs_1 = create_bdtopo_building(polygon_geojson=bdtopo_1)

        bdtopo_2 = _rect(5, 5, 20, 20)
        bdtopo_cleabs_2 = create_bdtopo_building(polygon_geojson=bdtopo_2)

        # Small RNB in the overlap region
        rnb = _rect(8, 8, 4, 4)  # well inside both BD TOPO buildings
        rnb_id = "RNB_INCLUDED_OVERLAP_BDTOPO"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(result["traitement"], "Batiments inclus")
        liens = set(result["liens_vers_batiment"].split("/"))
        self.assertEqual(liens, {bdtopo_cleabs_1, bdtopo_cleabs_2})

    # -------------------------------------------------------------------------
    # Already paired by earlier step (semantic): should NOT appear as included
    # -------------------------------------------------------------------------

    def test_already_paired_by_semantic(self):
        """A building paired semantically in an earlier step should NOT appear
        as 'Batiments inclus' even if geometrically it would qualify.
        """
        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=WAREHOUSE_30x30)

        rnb_id = "RNB_INCLUDED_SEMANTIC_FIRST"
        create_rnb_building(
            polygon_geojson=HOUSE_8x8_CENTERED,
            identifiant_rnb=rnb_id,
            bdtopo_ids=[bdtopo_cleabs],
        )

        run_pairing_after_rnb_update()

        result = self._get_pairing_result(rnb_id)
        self.assertEqual(
            result["traitement"],
            "Croisement sémantique",
            "Should be paired semantically, not as included",
        )

    # -------------------------------------------------------------------------
    # RNB building too small: -0.5m buffer collapses the geometry
    # -------------------------------------------------------------------------

    def test_very_small_rnb_buffer_collapses(self):
        """An RNB building smaller than 1x1m. After -0.5m buffer, the geometry
        collapses to nothing. Should NOT pair as 'Batiments inclus'.

        BD TOPO: 20x20m. RNB: 0.8x0.8m centered at (10, 10).
        After -0.5m buffer: geometry collapses (width < 2 * 0.5m = 1m required).
        """
        bdtopo = _rect(0, 0, 20, 20)
        create_bdtopo_building(polygon_geojson=bdtopo)

        rnb = _rect(9.6, 9.6, 0.8, 0.8)
        rnb_id = "RNB_INCLUDED_TINY"
        create_rnb_building(polygon_geojson=rnb, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_not_paired_as_included(rnb_id)
