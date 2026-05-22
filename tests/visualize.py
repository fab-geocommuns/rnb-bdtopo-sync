"""
Visualization helper for sharing test scenarios on geojson.io.

Use `visualize_test()` inside a test to open the browser on geojson.io with
all current RNB + BDTopo buildings (plus pairing outcomes on BDTopo features).
"""

import functools
import json
import webbrowser
from typing import Optional
from urllib.parse import quote

from db import dictfetchall, get_connection, get_cursor

GEOJSONIO_URL = "https://geojson.io/#data=data:application/json,"
RNB_COLOR = "#1c5cf2"
BDTOPO_COLOR = "#63ef06"
PAIRING_RESULTS_TABLE = "processus_divers.rnb_batiments_rnb_traites_creation"


def visualize_test(title: Optional[str] = None) -> None:
    """Open geojson.io showing all RNB + BDTopo buildings in the test DB.

    Each BDTopo feature carries the list of RNBs paired to it (with their
    `traitement`), when pairing has run.
    """
    features = _fetch_rnb_features() + _fetch_bdtopo_features()

    collection: dict = {"type": "FeatureCollection", "features": features}
    if title is not None:
        collection["name"] = title

    webbrowser.open(GEOJSONIO_URL + quote(json.dumps(collection)))


def visu(func):
    """Test-method decorator: open geojson.io after the test runs.

    Fires in a `finally`, so the visualization opens on failure too.
    Title defaults to the test method name.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            visualize_test(title=func.__name__)

    return wrapper


def _fetch_rnb_features() -> list[dict]:
    sql = """
        SELECT
            identifiant_rnb,
            ST_AsGeoJSON(
                ST_Transform(ST_SetSRID(geometrie_enveloppe, 2154), 4326)
            ) AS geom
        FROM public.batiment_rnb_lien_bdtopo
    """
    with get_cursor() as cursor:
        rows = dictfetchall(cursor, sql)

    return [
        {
            "type": "Feature",
            "geometry": json.loads(row["geom"]),
            "properties": {
                "layer": "rnb",
                "identifiant_rnb": row["identifiant_rnb"],
                "stroke": RNB_COLOR,
                "fill": RNB_COLOR,
            },
        }
        for row in rows
    ]


def _fetch_bdtopo_features() -> list[dict]:
    pairings = _fetch_pairings_by_bdtopo()

    sql = """
        SELECT
            cleabs,
            ST_AsGeoJSON(
                ST_Transform(ST_Force2D(ST_SetSRID(geometrie, 2154)), 4326)
            ) AS geom
        FROM public.batiment
    """
    with get_cursor() as cursor:
        rows = dictfetchall(cursor, sql)

    return [
        {
            "type": "Feature",
            "geometry": json.loads(row["geom"]),
            "properties": {
                "layer": "bdtopo",
                "cleabs": row["cleabs"],
                "paired_rnb": pairings.get(row["cleabs"], []),
                "stroke": BDTOPO_COLOR,
                "fill": BDTOPO_COLOR,
            },
        }
        for row in rows
    ]


def _fetch_pairings_by_bdtopo() -> dict[str, list[dict]]:
    """Return {bdtopo_cleabs: [{identifiant_rnb, traitement}, ...]}.

    Returns {} when the pairing-results table doesn't exist yet (i.e.
    visualize_test() was called before run_pairing_after_rnb_update()).
    """
    with get_connection() as conn:
        with get_cursor(conn) as cursor:
            cursor.execute("SELECT to_regclass(%s)", (PAIRING_RESULTS_TABLE,))
            if cursor.fetchone()[0] is None:
                return {}

        with get_cursor(conn) as cursor:
            rows = dictfetchall(
                cursor,
                f"""
                SELECT identifiant_rnb, liens_vers_batiment, traitement
                FROM {PAIRING_RESULTS_TABLE}
                WHERE liens_vers_batiment IS NOT NULL
                """,
            )

    result: dict[str, list[dict]] = {}
    for row in rows:
        result.setdefault(row["liens_vers_batiment"], []).append(
            {
                "identifiant_rnb": row["identifiant_rnb"],
                "traitement": row["traitement"],
            }
        )
    return result
