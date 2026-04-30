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

    def _assert_rnb_not_paired(self, identifiant_rnb):
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

    def _assert_bdtopo_not_paired(self, bdtopo_cleabs):
        """Assert that the BD TOPO building was not paired to any RNB building."""
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                rows = dictfetchall(
                    cursor,
                    """
                    SELECT identifiant_rnb, liens_vers_batiment
                    FROM processus_divers.rnb_batiments_rnb_traites_creation
                    WHERE liens_vers_batiment LIKE %s
                    """,
                    (f"%{bdtopo_cleabs}%",),
                )
        self.assertEqual(
            rows,
            [],
            f"{bdtopo_cleabs} should not be linked to any RNB building, got: {rows}",
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

    def test_split_rnb_with_adjacent(self):
        """
        A BD TOPO building split into 3 RNB buildings: each RNB should be paired to it.
        The catch is there is a lonely RNB building next to the 3 split. This one should be paired. Also it should be a problem to pair the 3 others
        """
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

        rnb_id_adjacent = "PAIRING_SPLIT_ADJACENT"
        create_rnb_building(
            polygon_geojson={
                "coordinates": [
                    [
                        [0.269355548478984, 44.56374816198118],
                        [0.26930492178385634, 44.56370468685353],
                        [0.2693268823703363, 44.563691190447344],
                        [0.2693766708745784, 44.5637375320762],
                        [0.269355548478984, 44.56374816198118],
                    ]
                ],
                "type": "Polygon",
            },
            identifiant_rnb=rnb_id_adjacent,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id_1, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_2, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_3, bdtopo_cleabs)

        self._assert_rnb_not_paired(rnb_id_adjacent)

    @unittest.expectedFailure
    def test_split_bdtopo_with_adjacent(self):
        """
        One RNB building covering 3 BD TOPO buildings: RNB should be paired to all 3.
        The catch is there is a lonely BD TOPO building adjacent to the 3 splits.
        The RNB should NOT be paired to the adjacent BD TOPO building.
        """
        rnb_id = "PAIRING_RNB_LARGE"
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

        bdtopo_cleabs_adjacent = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.269355548478984, 44.56374816198118],
                        [0.26930492178385634, 44.56370468685353],
                        [0.2693268823703363, 44.563691190447344],
                        [0.2693766708745784, 44.5637375320762],
                        [0.269355548478984, 44.56374816198118],
                    ]
                ],
            }
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs_1)
        self._assert_paired_to(rnb_id, bdtopo_cleabs_2)
        self._assert_paired_to(rnb_id, bdtopo_cleabs_3)
        self._assert_bdtopo_not_paired(bdtopo_cleabs_adjacent)

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
    def test_balcony_2(self):
        """A house with a BD TOPO balcony: RNB house should pair to BD TOPO house, not balcony."""

        # A house to link in bdtopo and rnb
        house_geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-0.357746, 49.320545],
                    [-0.357579, 49.320495],
                    [-0.357526, 49.320573],
                    [-0.357697, 49.320622],
                    [-0.357746, 49.320545],
                ]
            ],
        }

        house_cleabs = create_bdtopo_building(polygon_geojson=house_geojson)
        create_rnb_building(polygon_geojson=house_geojson, identifiant_rnb="HOUSE")

        # A bd topo balcony that should not be linked
        balcony_geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-0.357697, 49.320622],
                    [-0.357526, 49.320573],
                    [-0.357516, 49.320588],
                    [-0.357686, 49.320636],
                    [-0.357697, 49.320622],
                ]
            ],
        }

        balcony_cleabs = create_bdtopo_building(polygon_geojson=balcony_geojson)

        run_pairing_after_rnb_update()

        self._assert_paired_to("HOUSE", house_cleabs)
        self._assert_bdtopo_not_paired(balcony_cleabs)

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

        self._assert_rnb_not_paired(rnb_id)

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

        self._assert_rnb_not_paired(rnb_id)

    @unittest.expectedFailure
    def test_large_buildings_slightly_offset(self):
        """Two large buildings (warehouse-sized) slightly offset should be paired."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2685972751760346, 44.56416226182381],
                        [0.2685972751760346, 44.56352887720067],
                        [0.26986568001271394, 44.56352887720067],
                        [0.26986568001271394, 44.56416226182381],
                        [0.2685972751760346, 44.56416226182381],
                    ]
                ],
            }
        )

        rnb_id = "PAIRING_LARGE_OFFSET"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2686105813305062, 44.564161420997095],
                        [0.2686105813305062, 44.5635284024211],
                        [0.26988074061705447, 44.5635284024211],
                        [0.26988074061705447, 44.564161420997095],
                        [0.2686105813305062, 44.564161420997095],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs)

    def test_imperfect_rnb_split(self):
        """A BD TOPO building split into 3 RNB buildings with imperfect geometry: each RNB should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.27018081022004026, 44.56327404924565],
                        [0.27008203843237766, 44.563184893949426],
                        [0.2702331838725911, 44.56310049681559],
                        [0.2703340646663719, 44.563189652241164],
                        [0.27018081022004026, 44.56327404924565],
                    ]
                ],
            }
        )

        rnb_id_1 = "PAIRING_IMPERFECT_SPLIT_1"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2701809009116971, 44.5632754513411],
                        [0.27012151234083603, 44.56322348807211],
                        [0.27017100281651096, 44.56318822725598],
                        [0.2702309123397413, 44.56324278872029],
                        [0.2701809009116971, 44.5632754513411],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_1,
        )

        rnb_id_2 = "PAIRING_IMPERFECT_SPLIT_2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.270236698882826, 44.563246501275415],
                        [0.2701702651893072, 44.56318789918632],
                        [0.27023002036335697, 44.5631595998658],
                        [0.27028590902611427, 44.5632179515471],
                        [0.270236698882826, 44.563246501275415],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_2,
        )

        rnb_id_3 = "PAIRING_IMPERFECT_SPLIT_3"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.2702855575250851, 44.563218201983375],
                        [0.2702296688623278, 44.5631598503023],
                        [0.2701147280276075, 44.56321845241962],
                        [0.270082741434436, 44.56318439307657],
                        [0.27023388687464944, 44.56310074725238],
                        [0.27033547067046015, 44.56319040355032],
                        [0.2702855575250851, 44.563218201983375],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_3,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id_1, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_2, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_3, bdtopo_cleabs)

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

        self._assert_rnb_not_paired(rnb_id)

    def test_included_almost_similar(self):
        """An RNB building almost identical to a BD TOPO building (slightly included) should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "coordinates": [
                    [
                        [0.27060440801921004, 44.56324931522775],
                        [0.2708443026945986, 44.56324185901778],
                        [0.27084510771027226, 44.56333018636687],
                        [0.27080244187783364, 44.56332903925929],
                        [0.27080646695623045, 44.56339901278014],
                        [0.2706591490858159, 44.5634001598863],
                        [0.27064868388188756, 44.56333075992066],
                        [0.2706060180505574, 44.56332903925929],
                        [0.27060440801921004, 44.56324931522775],
                    ]
                ],
                "type": "Polygon",
            }
        )

        rnb_id = "PAIRING_INCLUDED_ALMOST_SIMILAR"
        create_rnb_building(
            polygon_geojson={
                "coordinates": [
                    [
                        [0.2706670336859247, 44.56340006605171],
                        [0.27065151352644534, 44.56324783795165],
                        [0.27084241148833144, 44.56324194048696],
                        [0.27084344616562817, 44.563329296621305],
                        [0.2708015417349827, 44.56332892803033],
                        [0.27080516310550706, 44.56339896028001],
                        [0.2706670336859247, 44.56340006605171],
                    ]
                ],
                "type": "Polygon",
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs)

    def test_rnb_split_and_missing_part(self):
        """A BD TOPO building split into 2 RNB buildings that don't fully cover it: both RNB should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "coordinates": [
                    [
                        [0.2705921448798563, 44.56340800976599],
                        [0.2705921448798563, 44.56325605166734],
                        [0.27102956278218926, 44.56325605166734],
                        [0.27102956278218926, 44.56340800976599],
                        [0.2705921448798563, 44.56340800976599],
                    ]
                ],
                "type": "Polygon",
            }
        )

        rnb_id_1 = "PAIRING_SPLIT_MISSING_1"
        create_rnb_building(
            polygon_geojson={
                "coordinates": [
                    [
                        [0.2705921448798563, 44.56325643832966],
                        [0.2708173662560114, 44.56325643832966],
                        [0.2708173662560114, 44.56338365009751],
                        [0.2705921448798563, 44.56338365009751],
                        [0.2705921448798563, 44.56325643832966],
                    ]
                ],
                "type": "Polygon",
            },
            identifiant_rnb=rnb_id_1,
        )

        rnb_id_2 = "PAIRING_SPLIT_MISSING_2"
        create_rnb_building(
            polygon_geojson={
                "coordinates": [
                    [
                        [0.27102956278218926, 44.56325643832966],
                        [0.27102956278218926, 44.563384036759004],
                        [0.2708173662560114, 44.563384036759004],
                        [0.2708173662560114, 44.56325643832966],
                        [0.27102956278218926, 44.56325643832966],
                    ]
                ],
                "type": "Polygon",
            },
            identifiant_rnb=rnb_id_2,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id_1, bdtopo_cleabs)
        self._assert_paired_to(rnb_id_2, bdtopo_cleabs)

    def test_almost_similar_quite_big(self):
        """A large RNB building almost identical to a large BD TOPO building should be paired to it."""
        bdtopo_cleabs = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.346201, 49.165509],
                        [-0.345998, 49.165575],
                        [-0.345878, 49.165428],
                        [-0.345837, 49.16544],
                        [-0.345912, 49.165538],
                        [-0.345745, 49.165591],
                        [-0.345664, 49.165491],
                        [-0.345603, 49.165513],
                        [-0.345589, 49.165495],
                        [-0.345651, 49.165474],
                        [-0.345591, 49.165399],
                        [-0.345762, 49.16534],
                        [-0.345823, 49.165416],
                        [-0.345861, 49.165405],
                        [-0.345743, 49.165258],
                        [-0.345952, 49.16519],
                        [-0.346062, 49.165332],
                        [-0.345997, 49.165357],
                        [-0.346018, 49.165385],
                        [-0.346084, 49.165365],
                        [-0.346201, 49.165509],
                    ]
                ],
            }
        )

        rnb_id = "PAIRING_ALMOST_SIMILAR_BIG"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.346168960310628, 49.16550589520197],
                        [-0.346062877513435, 49.16537206537627],
                        [-0.34600776193303, 49.165389795411286],
                        [-0.345973527297482, 49.16534845362302],
                        [-0.346028584766544, 49.16532982535275],
                        [-0.34592387246871, 49.165195957294934],
                        [-0.345742611371366, 49.16525768839049],
                        [-0.345857666783007, 49.16540296726194],
                        [-0.345795992565696, 49.165425378870935],
                        [-0.345734837131511, 49.16534969075456],
                        [-0.345591174230236, 49.165398678036674],
                        [-0.345650959728879, 49.165474404304106],
                        [-0.345589169193764, 49.16549501930511],
                        [-0.3456026586868, 49.16551264157961],
                        [-0.34566439116465, 49.16549112832181],
                        [-0.345736006277198, 49.165580023636224],
                        [-0.34587966954459, 49.16553103619018],
                        [-0.345808054253157, 49.16544214095687],
                        [-0.345872584201716, 49.16542144967387],
                        [-0.34598769842051, 49.16556762664735],
                        [-0.346168960310628, 49.16550589520197],
                    ]
                ],
            },
            identifiant_rnb=rnb_id,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id, bdtopo_cleabs)

    @unittest.expectedFailure
    def test_complex_cut(self):
        """Complex cut: multiple RNB buildings paired to 2 BD TOPO buildings."""
        bdtopo_1 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.310043, 49.152885],
                        [-0.30972, 49.153583],
                        [-0.309727, 49.153583],
                        [-0.309684, 49.153683],
                        [-0.309959, 49.153737],
                        [-0.309945, 49.153771],
                        [-0.309603, 49.1537],
                        [-0.309903, 49.15306],
                        [-0.309815, 49.153043],
                        [-0.309908, 49.152835],
                        [-0.309991, 49.152849],
                        [-0.309979, 49.152876],
                        [-0.310043, 49.152885],
                    ]
                ],
            }
        )

        bdtopo_2 = create_bdtopo_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.309727, 49.153583],
                        [-0.30972, 49.153583],
                        [-0.310043, 49.152885],
                        [-0.310247, 49.152443],
                        [-0.310532, 49.1525],
                        [-0.310026, 49.153598],
                        [-0.309959, 49.153737],
                        [-0.309684, 49.153683],
                        [-0.309727, 49.153583],
                    ]
                ],
            }
        )

        rnb_id_WMPFREC6FB3J = "WMPFREC6FB3J"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.309720118712728, 49.15358296255273],
                        [-0.310043142958068, 49.15288478433448],
                        [-0.310246723860939, 49.15244275149927],
                        [-0.310531600399184, 49.15249970293949],
                        [-0.31002564007269, 49.15359795388214],
                        [-0.31002361511176, 49.15360219984744],
                        [-0.310006, 49.153601399999985],
                        [-0.309724519283961, 49.15358847629823],
                        [-0.309726966188627, 49.153582774185104],
                        [-0.309720118712728, 49.15358296255273],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_WMPFREC6FB3J,
        )

        rnb_id_81T48E4RTKRM = "81T48E4RTKRM"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.310057566231961, 49.152852892455854],
                        [-0.310004434790733, 49.15283725688962],
                        [-0.310017775803189, 49.152809894084434],
                        [-0.309976232048394, 49.15280383815488],
                        [-0.310004168664373, 49.1527472783125],
                        [-0.310098556535491, 49.152764478388654],
                        [-0.310057566231961, 49.152852892455854],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_81T48E4RTKRM,
        )

        rnb_id_XCE6A7WNWQ35 = "XCE6A7WNWQ35"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.310383319786713, 49.15281963360428],
                        [-0.310409512144061, 49.15282161249677],
                        [-0.310441097133404, 49.15275775328156],
                        [-0.31041473246523, 49.1527530795602],
                        [-0.310383319786713, 49.15281963360428],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_XCE6A7WNWQ35,
        )

        rnb_id_WSQEDQA6HDFK = "WSQEDQA6HDFK"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.309911851453232, 49.15304137236311],
                        [-0.309811698032422, 49.153019831450685],
                        [-0.309834042154788, 49.15296162580072],
                        [-0.309938074152681, 49.152979460553645],
                        [-0.309987272536247, 49.152869224115534],
                        [-0.310043142958068, 49.15288478433448],
                        [-0.309720118712728, 49.15358296255273],
                        [-0.309726966188627, 49.153582774185104],
                        [-0.309724519283961, 49.15358847629823],
                        [-0.3096989, 49.153587299999984],
                        [-0.309656377301783, 49.153585348158046],
                        [-0.309911851453232, 49.15304137236311],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_WSQEDQA6HDFK,
        )

        rnb_id_C649ZDR1TR4A = "C649ZDR1TR4A"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.3100098, 49.15360149999998],
                        [-0.310023647797466, 49.15360213131163],
                        [-0.309959106196719, 49.153737462616554],
                        [-0.309683982271452, 49.15368294101475],
                        [-0.309724532734975, 49.15358844495285],
                        [-0.3100098, 49.15360149999998],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_C649ZDR1TR4A,
        )

        rnb_id_YZECYZTY6XW2 = "YZECYZTY6XW2"
        create_rnb_building(
            polygon_geojson={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.3097017, 49.153587399999964],
                        [-0.309724532734975, 49.15358844495285],
                        [-0.309683982271452, 49.15368294101475],
                        [-0.309959106196719, 49.153737462616554],
                        [-0.30994479749642, 49.15377115101103],
                        [-0.309602731344627, 49.15369957382048],
                        [-0.309656388329348, 49.15358532467755],
                        [-0.3097017, 49.153587399999964],
                    ]
                ],
            },
            identifiant_rnb=rnb_id_YZECYZTY6XW2,
        )

        run_pairing_after_rnb_update()

        self._assert_paired_to(rnb_id_YZECYZTY6XW2, bdtopo_1)
        self._assert_paired_to(rnb_id_WSQEDQA6HDFK, bdtopo_1)
        self._assert_paired_to(rnb_id_C649ZDR1TR4A, bdtopo_2)
        self._assert_paired_to(rnb_id_WMPFREC6FB3J, bdtopo_2)

        self._assert_rnb_not_paired(rnb_id_81T48E4RTKRM)
        self._assert_rnb_not_paired(rnb_id_XCE6A7WNWQ35)
