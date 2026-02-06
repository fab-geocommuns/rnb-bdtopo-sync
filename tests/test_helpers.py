"""
Tests for the helpers module in tests directory.
"""

import unittest
from datetime import datetime, timedelta
from db import setup_db, dictfetchall, get_connection, get_cursor
from tests.helpers import create_rnb_building, create_bdtopo_building


class TestCreateRnbBuilding(unittest.TestCase):
    """Test the create_rnb_building helper function."""

    def setUp(self):
        """Set up test database before each test."""
        setup_db()

    def test_create_rnb_building_with_geojson(self):
        """Test creating an RNB building with custom GeoJSON geometry."""
        # Define a custom polygon in GeoJSON format (WGS84 coordinates)
        custom_polygon = {
            "type": "Polygon",
            "coordinates": [
                [
                    [2.3522, 48.8566],  # Paris coordinates (lon, lat)
                    [2.3532, 48.8566],
                    [2.3532, 48.8576],
                    [2.3522, 48.8576],
                    [2.3522, 48.8566],
                ]
            ],
        }

        # Create building with BD TOPO IDs for semantic matching
        bdtopo_ids = ["BATIMENT_TEST_001", "BATIMENT_TEST_002"]

        # Create the building
        cleabs = create_rnb_building(
            polygon_geojson=custom_polygon,
            identifiant_rnb="TEST_RNB_ID",
            bdtopo_ids=bdtopo_ids,
            status="constructed",
            event_type="construction",
        )

        # Verify the building was created
        self.assertIsNotNone(cleabs)
        self.assertTrue(cleabs.startswith("BAT_RNB_"))

        # Query the database to verify all fields
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT
                        cleabs,
                        identifiant_rnb,
                        informations_rnb,
                        status,
                        event_type,
                        gcms_detruit,
                        ST_AsGeoJSON(geometrie_enveloppe) as geometry_json,
                        ST_AsText(geometrie) as point_wkt,
                        ST_SRID(geometrie_enveloppe) as srid
                    FROM public.batiment_rnb_lien_bdtopo
                    WHERE cleabs = %(cleabs)s
                """

                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                # Verify we got exactly one result
                self.assertEqual(len(result), 1)

                building = result[0]

                # Check basic fields
                self.assertEqual(building["cleabs"], cleabs)
                self.assertEqual(building["identifiant_rnb"], "TEST_RNB_ID")
                self.assertEqual(building["status"], "constructed")
                self.assertEqual(building["event_type"], "construction")
                self.assertFalse(building["gcms_detruit"])

                # Check SRID (should be 0 for local coordinates)
                self.assertEqual(building["srid"], 0)

                # Check informations_rnb JSON structure
                import json

                info_rnb = json.loads(building["informations_rnb"])
                self.assertIn("ext_ids", info_rnb)
                self.assertIn("created_at", info_rnb)
                self.assertIn("updated_at", info_rnb)

                # Verify ext_ids (BD TOPO IDs converted to ext_ids format)
                self.assertEqual(len(info_rnb["ext_ids"]), 2)
                self.assertEqual(info_rnb["ext_ids"][0]["id"], "BATIMENT_TEST_001")
                self.assertEqual(info_rnb["ext_ids"][0]["source"], "bdtopo")
                self.assertEqual(info_rnb["ext_ids"][1]["id"], "BATIMENT_TEST_002")
                self.assertEqual(info_rnb["ext_ids"][1]["source"], "bdtopo")

                # Verify geometry was stored correctly
                # Note: PostGIS may convert Polygon to MultiPolygon based on column type
                geometry = json.loads(building["geometry_json"])
                self.assertIn(geometry["type"], ["Polygon", "MultiPolygon"])
                self.assertIsNotNone(geometry["coordinates"])

                # Verify point geometry was generated
                self.assertIsNotNone(building["point_wkt"])
                self.assertTrue(building["point_wkt"].startswith("POINT"))

    def test_create_rnb_building_multipolygon(self):
        """Test creating an RNB building with MultiPolygon geometry."""
        # Define a MultiPolygon (building with multiple parts)
        multipolygon = {
            "type": "MultiPolygon",
            "coordinates": [
                [
                    [
                        [2.3522, 48.8566],
                        [2.3527, 48.8566],
                        [2.3527, 48.8571],
                        [2.3522, 48.8571],
                        [2.3522, 48.8566],
                    ]
                ],
                [
                    [
                        [2.3532, 48.8566],
                        [2.3537, 48.8566],
                        [2.3537, 48.8571],
                        [2.3532, 48.8571],
                        [2.3532, 48.8566],
                    ]
                ],
            ],
        }

        # Create the building
        cleabs = create_rnb_building(
            polygon_geojson=multipolygon,
            identifiant_rnb="TEST_MULTIPOLYGON_ID",
        )

        # Verify the building was created
        self.assertIsNotNone(cleabs)

        # Query to verify geometry type
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT
                        ST_GeometryType(geometrie_enveloppe) as geom_type,
                        ST_NumGeometries(geometrie_enveloppe) as num_parts
                    FROM public.batiment_rnb_lien_bdtopo
                    WHERE cleabs = %(cleabs)s
                """

                result = dictfetchall(cursor, query, {"cleabs": cleabs})
                self.assertEqual(len(result), 1)

                # Verify it's a MultiPolygon with 2 parts
                self.assertEqual(result[0]["geom_type"], "ST_MultiPolygon")
                self.assertEqual(result[0]["num_parts"], 2)


class TestCreateBdtopoBuilding(unittest.TestCase):
    """Test the create_bdtopo_building helper function."""

    def setUp(self):
        """Set up test database before each test."""
        setup_db()

    def test_create_bdtopo_building_with_defaults(self):
        """Test creating a BD TOPO building with default parameters."""
        # Define a simple MultiPolygon Z in WKT format
        polygon_wkt = """
            MULTIPOLYGON Z(((
                0 0 0,
                10 0 0,
                10 10 0,
                0 10 0,
                0 0 0
            )))
        """

        # Create the building with defaults
        cleabs = create_bdtopo_building(polygon_wkt=polygon_wkt)

        # Verify the building was created
        self.assertIsNotNone(cleabs)
        self.assertTrue(cleabs.startswith("BATIMENT"))

        # Query the database to verify all fields
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT
                        cleabs,
                        gcms_detruit,
                        gcms_date_creation,
                        gcms_date_destruction,
                        ST_AsText(geometrie) as geometry_wkt,
                        ST_SRID(geometrie) as srid,
                        ST_GeometryType(geometrie) as geom_type,
                        nature,
                        usage_1
                    FROM public.batiment
                    WHERE cleabs = %(cleabs)s
                """

                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                # Verify we got exactly one result
                self.assertEqual(len(result), 1)

                building = result[0]

                # Check basic fields
                self.assertEqual(building["cleabs"], cleabs)
                self.assertFalse(building["gcms_detruit"])
                self.assertIsNotNone(building["gcms_date_creation"])
                self.assertIsNone(building["gcms_date_destruction"])

                # Check SRID (should be 0 by default)
                self.assertEqual(building["srid"], 0)

                # Check geometry type
                self.assertEqual(building["geom_type"], "ST_MultiPolygon")

                # Check default values for non-pairing fields
                self.assertEqual(building["nature"], "Indifférenciée")
                self.assertEqual(building["usage_1"], "Indifférencié")

    def test_create_bdtopo_building_with_custom_cleabs(self):
        """Test creating a BD TOPO building with a custom cleabs."""
        polygon_wkt = "MULTIPOLYGON Z(((0 0 0, 5 0 0, 5 5 0, 0 5 0, 0 0 0)))"
        custom_cleabs = "BATIMENT_CUSTOM_12345"

        # Create with custom cleabs
        cleabs = create_bdtopo_building(
            polygon_wkt=polygon_wkt, cleabs=custom_cleabs
        )

        # Verify the custom cleabs was used
        self.assertEqual(cleabs, custom_cleabs)

        # Verify in database
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = "SELECT cleabs FROM public.batiment WHERE cleabs = %(cleabs)s"
                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["cleabs"], custom_cleabs)

    def test_create_bdtopo_building_destroyed(self):
        """Test creating a destroyed BD TOPO building."""
        polygon_wkt = "MULTIPOLYGON Z(((0 0 0, 5 0 0, 5 5 0, 0 5 0, 0 0 0)))"
        destruction_date = datetime.now() - timedelta(days=5)

        # Create a destroyed building
        cleabs = create_bdtopo_building(
            polygon_wkt=polygon_wkt,
            gcms_detruit=True,
            gcms_date_destruction=destruction_date,
        )

        # Verify destruction flags
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT
                        gcms_detruit,
                        gcms_date_destruction
                    FROM public.batiment
                    WHERE cleabs = %(cleabs)s
                """
                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                self.assertEqual(len(result), 1)
                building = result[0]

                # Check destruction flag is True
                self.assertTrue(building["gcms_detruit"])

                # Check destruction date (compare with some tolerance for DB precision)
                self.assertIsNotNone(building["gcms_date_destruction"])
                delta = abs(
                    building["gcms_date_destruction"] - destruction_date
                ).total_seconds()
                self.assertLess(delta, 2)  # Within 2 seconds

    def test_create_bdtopo_building_with_custom_creation_date(self):
        """Test creating a BD TOPO building with custom creation date."""
        polygon_wkt = "MULTIPOLYGON Z(((0 0 0, 5 0 0, 5 5 0, 0 5 0, 0 0 0)))"
        creation_date = datetime.now() - timedelta(days=10)

        # Create with custom creation date
        cleabs = create_bdtopo_building(
            polygon_wkt=polygon_wkt, gcms_date_creation=creation_date
        )

        # Verify creation date
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT gcms_date_creation
                    FROM public.batiment
                    WHERE cleabs = %(cleabs)s
                """
                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                self.assertEqual(len(result), 1)

                # Check creation date (with tolerance for DB precision)
                db_creation_date = result[0]["gcms_date_creation"]
                delta = abs(db_creation_date - creation_date).total_seconds()
                self.assertLess(delta, 2)  # Within 2 seconds

    def test_create_bdtopo_building_srid_enforced(self):
        """Test that SRID is enforced to 0 by the database schema."""
        polygon_wkt = """
            MULTIPOLYGON Z(((
                0 0 0,
                10 0 0,
                10 10 0,
                0 10 0,
                0 0 0
            )))
        """

        # Create with default SRID (0)
        cleabs = create_bdtopo_building(polygon_wkt=polygon_wkt)

        # Verify SRID is 0 (enforced by schema constraint)
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT ST_SRID(geometrie) as srid
                    FROM public.batiment
                    WHERE cleabs = %(cleabs)s
                """
                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]["srid"], 0)

    def test_create_bdtopo_building_geometry_operations(self):
        """Test that geometry supports pairing operations (area, perimeter, npoints)."""
        # Create a rectangular building 10x20 meters
        polygon_wkt = """
            MULTIPOLYGON Z(((
                0 0 0,
                20 0 0,
                20 10 0,
                0 10 0,
                0 0 0
            )))
        """

        cleabs = create_bdtopo_building(polygon_wkt=polygon_wkt)

        # Verify geometry properties used in pairing
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT
                        ST_Area(geometrie) as area,
                        ST_Perimeter(geometrie) as perimeter,
                        ST_NPoints(geometrie) as npoints,
                        FLOOR(ST_Area(geometrie))::integer as area_floor,
                        FLOOR(ST_Perimeter(geometrie))::integer as perimeter_floor
                    FROM public.batiment
                    WHERE cleabs = %(cleabs)s
                """
                result = dictfetchall(cursor, query, {"cleabs": cleabs})

                self.assertEqual(len(result), 1)
                building = result[0]

                # Verify area (10 * 20 = 200 m²)
                self.assertAlmostEqual(building["area"], 200.0, delta=0.1)
                self.assertEqual(building["area_floor"], 200)

                # Verify perimeter (2 * (10 + 20) = 60 m)
                self.assertAlmostEqual(building["perimeter"], 60.0, delta=0.1)
                self.assertEqual(building["perimeter_floor"], 60)

                # Verify number of points (5 points including closure)
                self.assertEqual(building["npoints"], 5)

    def test_create_bdtopo_building_supports_spatial_operations(self):
        """Test that created buildings support spatial operations used in pairing."""
        # Create two overlapping buildings
        polygon_wkt_1 = "MULTIPOLYGON Z(((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)))"
        polygon_wkt_2 = "MULTIPOLYGON Z(((5 5 0, 15 5 0, 15 15 0, 5 15 0, 5 5 0)))"

        cleabs_1 = create_bdtopo_building(polygon_wkt=polygon_wkt_1)
        cleabs_2 = create_bdtopo_building(polygon_wkt=polygon_wkt_2)

        # Test spatial operations used in pairing
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                query = """
                    SELECT
                        ST_Intersects(b1.geometrie, b2.geometrie) as intersects,
                        ST_Area(ST_Intersection(b1.geometrie, b2.geometrie)) as intersection_area,
                        ST_Contains(b1.geometrie, ST_Buffer(b2.geometrie, -0.5)) as contains_buffered,
                        ST_DWithin(b1.geometrie, b2.geometrie, 20) as within_20m,
                        ST_Equals(b1.geometrie, b2.geometrie) as equals
                    FROM public.batiment b1, public.batiment b2
                    WHERE b1.cleabs = %(cleabs_1)s
                      AND b2.cleabs = %(cleabs_2)s
                """
                result = dictfetchall(
                    cursor, query, {"cleabs_1": cleabs_1, "cleabs_2": cleabs_2}
                )

                self.assertEqual(len(result), 1)
                ops = result[0]

                # Verify spatial relationships
                self.assertTrue(ops["intersects"])  # Buildings overlap
                self.assertGreater(
                    ops["intersection_area"], 0
                )  # Have intersection area
                self.assertFalse(
                    ops["contains_buffered"]
                )  # One doesn't contain the other
                self.assertTrue(ops["within_20m"])  # Within 20m of each other
                self.assertFalse(ops["equals"])  # Not equal geometries


if __name__ == "__main__":
    unittest.main()
