"""
Method-agnostic pairing tests.

These tests verify only whether pairing occurred and with the right building,
regardless of which pairing method was used (semantic, similar, included, etc.).
"""

import unittest
from db import dictfetchall, setup_db, get_connection, get_cursor
from pairing import run_pairing_after_rnb_update
from tests.helpers import create_rnb_building, create_bdtopo_building


class TestResultOnly(unittest.TestCase):

    def setUp(self):
        setup_db()

    def _get_pairing_result(self, identifiant_rnb):
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                rows = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment, traitement
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (identifiant_rnb,),
                )
        self.assertEqual(
            len(rows), 1, f"Expected exactly one result for {identifiant_rnb}"
        )
        return rows[0]

    def _assert_paired_to(self, identifiant_rnb, expected_bdtopo_cleabs):
        """Assert that the RNB building was paired to the given BD TOPO building."""
        result = self._get_pairing_result(identifiant_rnb)
        liens = result["liens_vers_batiment"]
        self.assertTrue(
            liens,
            f"{identifiant_rnb} should be paired but liens_vers_batiment is empty",
        )
        self.assertIn(
            expected_bdtopo_cleabs,
            liens.split("/"),
            f"{identifiant_rnb} should be paired to {expected_bdtopo_cleabs}, got: {liens}",
        )

    def _assert_not_paired(self, identifiant_rnb):
        """Assert that the RNB building was not paired to any BD TOPO building."""
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                rows = dictfetchall(
                    cursor,
                    """
                    SELECT liens_vers_batiment
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE identifiant_rnb = %s
                    """,
                    (identifiant_rnb,),
                )
        if rows:
            self.assertEqual(
                rows[0]["liens_vers_batiment"],
                "",
                f"{identifiant_rnb} should not be paired, got: {rows[0]['liens_vers_batiment']}",
            )

    @unittest.expectedFailure
    def test_almost_similar(self):
        """An RNB building overlapping a BD TOPO building should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26973895632022504, 44.56391072019835],
                        [0.2696651882964147, 44.56385591034385],
                        [0.26977478650482567, 44.563790588942766],
                        [0.2697642482147842, 44.563775572518665],
                        [0.26985066218716725, 44.56372451664663],
                        [0.26993286084254464, 44.56379884797485],
                        [0.26973895632022504, 44.56391072019835],
                    ]
                ],
            }
        )

        rnb_id = "PAIRING_OVERLAP"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26974003527263335, 44.563907941990834],
                        [0.26966854872691215, 44.563855512079726],
                        [0.26974704375714964, 44.5638140674497],
                        [0.26972321490885065, 44.56379459345695],
                        [0.2698528718778448, 44.563723688094285],
                        [0.2699369736959625, 44.56380008612254],
                        [0.26974003527263335, 44.563907941990834],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs)

    def test_very_similar(self):
        """An RNB building very similar to a BD TOPO building should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2700627176617161, 44.563731106005235],
                        [0.2699288710574024, 44.563641444602524],
                        [0.270016545496901, 44.56358711064837],
                        [0.2701026635859307, 44.563532037405196],
                        [0.27022665330093787, 44.563620590120365],
                        [0.27014364791429557, 44.56367492404323],
                        [0.2700627176617161, 44.563731106005235],
                    ]
                ],
            }
        )

        rnb_id = "PAIRING_VERY_SIMILAR"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.27006146595536507, 44.563732814425066],
                        [0.26992685029043173, 44.563640269932336],
                        [0.27010318893951535, 44.56353089897934],
                        [0.27022835789099986, 44.5636206392638],
                        [0.27006146595536507, 44.563732814425066],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs)

    def test_similar(self):
        """An RNB building with the same geometry as a BD TOPO building should be paired to it."""
        geom = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.26941720565881155, 44.563749733180885],
                    [0.26937112149030895, 44.56371033285373],
                    [0.26954482335767693, 44.563599203583095],
                    [0.26959090752711745, 44.56363708858572],
                    [0.26941720565881155, 44.563749733180885],
                ]
            ],
        }

        bdtopo_cleabs = create_bdtopo_building(polygon_geojson=geom)

        rnb_id = "PAIRING_SIMILAR"
        create_rnb_building(polygon_geojson=geom, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs)

    def test_split_rnb(self):
        """A BD TOPO building split into 3 RNB buildings: each RNB should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2691782961799163, 44.563850477701294],
                        [0.2691235108346177, 44.563804620902005],
                        [0.2693051070148442, 44.5637041725486],
                        [0.2693556781031248, 44.56374757280952],
                        [0.2691782961799163, 44.563850477701294],
                    ]
                ],
            }
        )

        rnb_id_1 = "PAIRING_SPLIT_1"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2692974447281813, 44.56378141953155],
                        [0.26924572429734894, 44.56373747338091],
                        [0.2693051070148442, 44.5637041725486],
                        [0.2693560612173087, 44.563748118725044],
                        [0.2692974447281813, 44.56378141953155],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_1,
        )

        rnb_id_2 = "PAIRING_SPLIT_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26923806201168077, 44.5638160851052],
                        [0.26918174420859486, 44.56377241193809],
                        [0.2692461074115329, 44.56373692746598],
                        [0.2692970616140258, 44.56378169248882],
                        [0.26923806201168077, 44.5638160851052],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        rnb_id_3 = "PAIRING_SPLIT_3"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26917867929412864, 44.56385075065893],
                        [0.26912389394877323, 44.563804620902005],
                        [0.26918174420859486, 44.56377241193809],
                        [0.26923806201168077, 44.5638160851052],
                        [0.26917867929412864, 44.56385075065893],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_3,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id_1, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_2, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_3, bdtopo_cleabs)

    @unittest.expectedFailure
    def test_split_bdtopo(self):
        """An RNB building that covers 3 BD TOPO buildings: should be paired to all 3."""
        bdtopo_cleabs_1 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2692974447281813, 44.56378141953155],
                        [0.26924572429734894, 44.56373747338091],
                        [0.2693051070148442, 44.5637041725486],
                        [0.2693560612173087, 44.563748118725044],
                        [0.2692974447281813, 44.56378141953155],
                    ]
                ],
            }
        )

        bdtopo_cleabs_2 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26923806201168077, 44.5638160851052],
                        [0.26918174420859486, 44.56377241193809],
                        [0.2692461074115329, 44.56373692746598],
                        [0.2692970616140258, 44.56378169248882],
                        [0.26923806201168077, 44.5638160851052],
                    ]
                ],
            }
        )

        bdtopo_cleabs_3 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26917867929412864, 44.56385075065893],
                        [0.26912389394877323, 44.563804620902005],
                        [0.26918174420859486, 44.56377241193809],
                        [0.26923806201168077, 44.5638160851052],
                        [0.26917867929412864, 44.56385075065893],
                    ]
                ],
            }
        )

        rnb_id = "PAIRING_SPLIT_BDTOPO"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2691782961799163, 44.563850477701294],
                        [0.2691235108346177, 44.563804620902005],
                        [0.2693051070148442, 44.5637041725486],
                        [0.2693556781031248, 44.56374757280952],
                        [0.2691782961799163, 44.563850477701294],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs_1)
        self._assert_paired_to(rnb_id, bdtopo_cleabs_2)
        self._assert_paired_to(rnb_id, bdtopo_cleabs_3)

    @unittest.expectedFailure
    def test_balcony(self):
        """A small BD TOPO balcony overlapping an RNB building should NOT be paired to it."""
        create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26938870507581214, 44.564049307454695],
                        [0.2693831186439297, 44.564052541322326],
                        [0.2693314441497705, 44.56401273985733],
                        [0.2693384271900072, 44.56400925722761],
                        [0.26938870507581214, 44.564049307454695],
                    ]
                ],
            }
        )

        rnb_id = "PAIRING_BALCONY"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.26941122015580277, 44.564067559942146],
                        [0.26931487841338253, 44.563991945510395],
                        [0.26942461526431316, 44.56391633097991],
                        [0.26955238399327186, 44.56400075496353],
                        [0.26941122015580277, 44.564067559942146],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired(rnb_id)

    @unittest.expectedFailure
    def test_rnb_included(self):
        """A small RNB building fully inside a large BD TOPO building should NOT be paired to it."""
        LARGE_BDTOPO = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.2687113571020632, 44.56423494238916],
                    [0.26870312970157784, 44.56407765246857],
                    [0.26902948324936915, 44.56406495202117],
                    [0.2689595503457838, 44.564246665844024],
                    [0.2687113571020632, 44.56423494238916],
                ]
            ],
        }
        SMALL_RNB = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.26887719512609465, 44.56414885434447],
                    [0.26887656725693887, 44.564128276926795],
                    [0.26892051811066153, 44.564127382256515],
                    [0.26892114597976047, 44.56415556437065],
                    [0.26887719512609465, 44.56414885434447],
                ]
            ],
        }

        create_bdtopo_building(polygon_geojson=LARGE_BDTOPO)

        rnb_id = "PAIRING_RNB_INCLUDED"
        create_rnb_building(polygon_geojson=SMALL_RNB, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_not_paired(rnb_id)

    def test_bdtopo_included(self):
        """A small BD TOPO building fully inside a large RNB building should NOT be paired to it."""
        SMALL_BDTOPO = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.26887719512609465, 44.56414885434447],
                    [0.26887656725693887, 44.564128276926795],
                    [0.26892051811066153, 44.564127382256515],
                    [0.26892114597976047, 44.56415556437065],
                    [0.26887719512609465, 44.56414885434447],
                ]
            ],
        }
        LARGE_RNB = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.2687113571020632, 44.56423494238916],
                    [0.26870312970157784, 44.56407765246857],
                    [0.26902948324936915, 44.56406495202117],
                    [0.2689595503457838, 44.564246665844024],
                    [0.2687113571020632, 44.56423494238916],
                ]
            ],
        }

        create_bdtopo_building(polygon_geojson=SMALL_BDTOPO)

        rnb_id = "PAIRING_BDTOPO_INCLUDED"
        create_rnb_building(polygon_geojson=LARGE_RNB, identifiant_rnb=rnb_id)

        run_pairing_after_rnb_update()

        self._assert_not_paired(rnb_id)

    def test_rnb_point_included(self):
        """A point-like RNB building (~1x1m) fully inside a large BD TOPO building should NOT be paired to it."""
        # Large BD TOPO (~30x20m warehouse)
        create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2687113571020632, 44.56423494238916],
                        [0.26870312970157784, 44.56407765246857],
                        [0.26902948324936915, 44.56406495202117],
                        [0.2689595503457838, 44.564246665844024],
                        [0.2687113571020632, 44.56423494238916],
                    ]
                ],
            }
        )

        # Point RNB: a single point inside the BD TOPO
        rnb_id = "PAIRING_RNB_POINT"
        create_rnb_building(
            polygon_geojson={
                "type": "Point",
                "coordinates": [0.26886067, 44.56415045],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_not_paired(rnb_id)
