"""
Helper functions to create test data for RNB and BD TOPO buildings.
These functions generate minimal but complete building records for testing the pairing process.
"""

import json
import random

import uuid
from datetime import datetime
from typing import Optional

from db import get_connection, get_cursor


def _generate_numrec() -> int:
    """Generate a random gcms_numrec."""
    return random.randint(1000000, 99999999)


def create_rnb_building(
    polygon_geojson: dict,
    identifiant_rnb: str,
    bdtopo_ids: Optional[list[str]] = None,
    status: str = "constructed",
    event_type: str = "construction",
) -> str:

    # Generate cleabs
    cleabs = f"BAT_RNB_{uuid.uuid4().hex[:16].upper()}"

    # Build informations_rnb JSON
    now_iso = datetime.now().isoformat()
    ext_ids = [
        {"id": bdtopo_id, "source": "bdtopo"} for bdtopo_id in (bdtopo_ids or [])
    ]

    informations_rnb_str = json.dumps(
        {"ext_ids": ext_ids, "created_at": now_iso, "updated_at": now_iso}
    )
    polygon_geojson_str = json.dumps(polygon_geojson)

    with get_connection() as conn:
        with get_cursor(conn) as cursor:
            insert_sql = """
                INSERT INTO public.batiment_rnb_lien_bdtopo (
                    cleabs,
                    identifiant_rnb,
                    informations_rnb,
                    geometrie_enveloppe,
                    gcms_detruit,
                    gcms_date_creation,
                    diffusion,
                    gcms_numrec,
                    gcms_territoire,
                    geometrie,
                    status,
                    event_type
                ) VALUES (
                    %(cleabs)s,
                    %(identifiant_rnb)s,
                    %(informations_rnb)s,
                    ST_SetSRID(ST_GeomFromGeoJSON(%(polygon_geojson)s), 0),
                    false,
                    NOW(),
                    true,
                    %(gcms_numrec)s,
                    'FXX',
                    ST_PointOnSurface(ST_SetSRID(ST_GeomFromGeoJSON(%(polygon_geojson)s), 0)),
                    %(status)s,
                    %(event_type)s
                )
            """

            cursor.execute(
                insert_sql,
                {
                    "cleabs": cleabs,
                    "identifiant_rnb": identifiant_rnb,
                    "informations_rnb": informations_rnb_str,
                    "polygon_geojson": polygon_geojson_str,
                    "gcms_numrec": _generate_numrec(),
                    "status": status,
                    "event_type": event_type,
                },
            )

    return cleabs


def create_bdtopo_building(
    polygon_wkt: str,
    cleabs: Optional[str] = None,
    gcms_detruit: bool = False,
    gcms_date_creation: Optional[datetime] = None,
    gcms_date_destruction: Optional[datetime] = None,
    srid: int = 0,
) -> str:
    """
    Create a BD TOPO building in the batiment table.

    Only includes parameters used by the pairing process:
    - cleabs (identifier)
    - geometrie (from polygon_wkt)
    - gcms_detruit (destruction flag)
    - gcms_date_creation (creation timestamp)
    - gcms_date_destruction (destruction timestamp)

    Parameters:
    -----------
    polygon_wkt : str
        WKT string for the building geometry (multipolygon Z).
        Example: "MULTIPOLYGON Z(((x1 y1 z1, x2 y2 z2, ...)))"
        If no Z values, they will be added as 0
    cleabs : str, optional
        BD TOPO identifier. Auto-generated if not provided
    gcms_detruit : bool
        Destruction flag. Default False (building exists)
    gcms_date_creation : datetime, optional
        Creation date. Default NOW()
    gcms_date_destruction : datetime, optional
        Destruction date. Only relevant if gcms_detruit is True
    srid : int
        Spatial reference ID (0 for local coordinates)

    Returns:
    --------
    str : The cleabs of the created building
    """
    # Generate cleabs if not provided
    if cleabs is None:
        cleabs = f"BATIMENT{uuid.uuid4().hex[:16].upper()}"

    with get_connection() as conn:
        with get_cursor(conn) as cursor:
            insert_sql = """
                INSERT INTO public.batiment (
                    cleabs,
                    nature,
                    usage_1,
                    construction_legere,
                    etat_de_l_objet,
                    gcms_detruit,
                    gcms_date_creation,
                    gcms_date_destruction,
                    diffusion,
                    methode_d_acquisition_planimetrique,
                    precision_planimetrique,
                    methode_d_acquisition_altimetrique,
                    precision_altimetrique,
                    hauteur,
                    gcms_numrec,
                    gcms_territoire,
                    geometrie,
                    nombre_d_etages
                ) VALUES (
                    %(cleabs)s,
                    'Indifférenciée',
                    'Indifférencié',
                    false,
                    'En service',
                    %(gcms_detruit)s,
                    COALESCE(%(gcms_date_creation)s, NOW()),
                    %(gcms_date_destruction)s,
                    true,
                    'BDTopo',
                    2.5,
                    'BDTopo',
                    1.5,
                    5.0,
                    %(gcms_numrec)s,
                    'FXX',
                    ST_SetSRID(ST_GeomFromText(%(polygon_wkt)s), %(srid)s),
                    NULL
                )
            """

            cursor.execute(
                insert_sql,
                {
                    "cleabs": cleabs,
                    "gcms_detruit": gcms_detruit,
                    "gcms_date_creation": gcms_date_creation,
                    "gcms_date_destruction": gcms_date_destruction,
                    "gcms_numrec": _generate_numrec(),
                    "polygon_wkt": polygon_wkt,
                    "srid": srid,
                },
            )

    return cleabs
