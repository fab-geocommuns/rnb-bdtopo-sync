import csv
import io
import re
import requests
from datetime import datetime
from typing import Iterator, Iterable
from db import get_cursor, SCHEMA_NAME


def getDiff_RNB_from_file(filename: str) -> Iterator[dict[str, str]]:

    print(f"Opening file: {filename}")

    def iterate_rows() -> Iterator[dict[str, str]]:
        with open(filename, mode="r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row

    return iterate_rows()


def getDiff_RNB_from_date(since: datetime) -> Iterator[dict[str, str]]:

    url = (
        "http://rnb-api.beta.gouv.fr/api/alpha/buildings/diff/?since="
        + since.isoformat()
    )

    print(f"Downloading: {url}")

    # On télécharge le fichier de diff du RNB
    response = requests.get(url)
    response.raise_for_status()

    def iterate_rows() -> Iterator[dict[str, str]]:
        # On garde le contenu du CSV en mémoire pour le parcourir
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)
        for row in reader:
            yield row

    return iterate_rows()


# todo : est-ce qu'on peut avoir une lecture plus simple ?
# Fonction pour extraire la date de début de sys_period
def extract_start_date(sys_period_str):
    match = re.match(r'\["?([\d\-:\.T+ ]+)', sys_period_str)
    if match:
        date_str = match.group(1).replace(" ", "T")
        if "+00" in date_str and not "+00:00" in date_str:
            date_str = date_str.replace("+00", "+00:00")
        return datetime.fromisoformat(date_str)
    return None


# fonction pour trier une liste de batiments et fournir en sortie version la plus récente selon le champ "sys_period"
#  prend en entrée une liste de batiment et retourne pour chaque rnb_id identiques l'élément avec la date la plus récente
def rnb_get_most_recent(liste_batiments: Iterable[dict[str, str]]):

    result = {}

    for bdg in liste_batiments:

        bdg["updated_at"] = extract_start_date(bdg["sys_period"])

        if bdg["rnb_id"] not in result:
            result[bdg["rnb_id"]] = bdg
        elif bdg["updated_at"] > result[bdg["rnb_id"]]["updated_at"]:
            result[bdg["rnb_id"]] = bdg

    # On retourne uniquement les valeurs (les bâtiments) et non les clés (les rnb_id)
    return list(result.values())


def calc_to_remove(rnb_diff) -> set:

    print("Calculating rows to remove in BD Topo ...")

    seen_rnb_ids = set()

    to_remove = set()

    for batiment_rnb in rnb_diff:

        if batiment_rnb["rnb_id"] in seen_rnb_ids:
            raise ValueError(
                f"Duplicate rnb_id found in rnb_diff: {batiment_rnb['rnb_id']}"
            )

        seen_rnb_ids.add(batiment_rnb["rnb_id"])

        # todo : est-ce qu'on veut vraiment casser les liens pour ces statuts ?
        # on veut casser les liens (ajouter à to_remove) si le batiment est inactif (is_active=0)
        # ou a un statut constructionProject ou canceledConstructionProject

        if batiment_rnb["is_active"] == "0" or batiment_rnb["status"] in [
            "constructionProject",
            "canceledConstructionProject",
        ]:
            to_remove.add(batiment_rnb["rnb_id"])

    return to_remove


"""
---------------------------------------
Remodelage des attributs des batiments RNB du fichier rnb_last_changes.csv pour calculer les champs created_at et updated_at :
    ENTREES : 
        - batiments_rnb_last_changes : liste des batiments correspondant à "last_changes"
    SORTIES :
        - batiments_rnb_last_changes_remodeled : liste des batis RNB agrémentés des champs created_at et updated_at
---------------------------------------
"""


def remodel_rnb_to_last_changes(batiments_rnb_last_changes):
    batiments_rnb_last_changes_remodeled = batiments_rnb_last_changes

    for batiment_rnb in batiments_rnb_last_changes_remodeled:
        date_sys = datetime.fromisoformat(batiment_rnb["sys_period"].split('"')[1])
        date_iso = date_sys.isoformat()

        if batiment_rnb["action"] == "create":

            batiment_rnb["created_at"] = date_iso
            batiment_rnb["updated_at"] = ""
        elif batiment_rnb["action"] == "update":
            # print(batiment_rnb['sys_period'].split('"')[1])
            batiment_rnb["created_at"] = ""
            batiment_rnb["updated_at"] = date_iso
        else:
            batiment_rnb["created_at"] = ""
            batiment_rnb["updated_at"] = date_iso
    return batiments_rnb_last_changes_remodeled


def setup_db():
    with get_cursor() as cursor:
        cursor.execute(f"CREATE EXTENSION IF NOT EXISTS postgis;")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        # create role invite if not exists
        cursor.execute(
            f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'invite') THEN CREATE ROLE invite NOINHERIT LOGIN; END IF; END $$;"
        )


def persist_last_changes(last_changes, table_creation_date: str):

    print("Remodeling RNB last changes ...")
    print(table_creation_date)

    _create_last_changes_table(table_creation_date)
    _insert_last_changes(last_changes)


def _create_last_changes_table(table_creation_date):

    with get_cursor() as cursor:

        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.rnb_last_changes cascade")

        create_sql = f"""\
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.rnb_last_changes (action varchar NULL,\
            rnb_id varchar NULL,\
            status varchar NULL,\
            sys_period varchar NULL,\
            point public.geometry NULL,\
            shape public.geometry NULL,\
            addresses_id varchar NULL,\
            ext_ids varchar NULL,\
            created_at varchar NULL,\
            updated_at varchar NULL,\
            event_type varchar NULL);\
        CREATE UNIQUE INDEX "{SCHEMA_NAME}_rnb_last_changes_rnb_id_pkey" ON {SCHEMA_NAME}.rnb_last_changes USING btree (rnb_id);\
        CREATE INDEX "{SCHEMA_NAME}_rnb_last_changes_POINT_idx" ON {SCHEMA_NAME}.rnb_last_changes USING gist (point);\
        GRANT SELECT ON {SCHEMA_NAME}.rnb_last_changes TO invite;\
        CREATE INDEX {SCHEMA_NAME}_rnb_last_changes_shape_idx ON {SCHEMA_NAME}.rnb_last_changes USING gist (shape);\
        COMMENT ON TABLE {SCHEMA_NAME}.rnb_last_changes IS %(table_creationc_comment)s;\
        """
        cursor.execute(
            create_sql,
            {"table_creationc_comment": f"Ne pas supprimer - {table_creation_date}"},
        )


def _insert_last_changes(last_changes):

    # convert last_changes to an in-memory csv
    # then use COPY

    last_changes_csv = io.StringIO()
    writer = csv.DictWriter(last_changes_csv, fieldnames=last_changes[0].keys())
    writer.writeheader()
    writer.writerows(last_changes)
    last_changes_csv.seek(0)

    with get_cursor() as cursor:
        cursor.copy_from(
            last_changes_csv,
            f"{SCHEMA_NAME}.rnb_last_changes",
            sep=",",
            columns=last_changes[0].keys(),
        )


def persist_to_remove(to_remove, table_creation_date: str):

    _create_to_remove_table(table_creation_date)
    _insert_to_remove(to_remove)


def _create_to_remove_table(table_creation_date):

    with get_cursor() as cursor:

        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.rnb_to_remove cascade;")

        create_sql = f"""\
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.rnb_to_remove (rnb_id varchar NULL);\
	CREATE UNIQUE INDEX "{SCHEMA_NAME}_rnb_to_remove_rnb_id_pkey" ON {SCHEMA_NAME}.rnb_to_remove USING btree (rnb_id);\
    GRANT SELECT ON {SCHEMA_NAME}.rnb_to_remove TO invite;\
    COMMENT ON TABLE {SCHEMA_NAME}.rnb_to_remove IS 'Ne pas supprimer - %(table_creation_date)s';
    """

        cursor.execute(create_sql, {"table_creation_date": table_creation_date})


def _insert_to_remove(to_remove: set):

    # convert to_remove to an in-memory csv
    # then use COPY

    to_remove_csv = io.StringIO()
    writer = csv.writer(to_remove_csv)
    for rnb_id in to_remove:
        writer.writerow([rnb_id])
    to_remove_csv.seek(0)

    with get_cursor() as cursor:
        cursor.copy_from(
            to_remove_csv,
            f"{SCHEMA_NAME}.rnb_to_remove",
            sep=",",
            columns=["rnb_id"],
        )
