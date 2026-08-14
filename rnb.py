import csv
import io
import re
import requests
from datetime import datetime
from typing import Iterator, Iterable


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
        "https://rnb-api.beta.gouv.fr/api/alpha/buildings/diff/?since="
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


def parse_sys_period(sys_period_str: str) -> tuple[datetime, datetime | None]:

    match = re.match(r'\["?([\d\-:\.T+ ]+)",?"?([\d\-:\.T+ ]+)?"?\)', sys_period_str)
    if match:

        def _str_to_date(date_str: str) -> datetime:
            date_str = date_str.replace(" ", "T")
            if "+00" in date_str and not "+00:00" in date_str:
                date_str = date_str.replace("+00", "+00:00")
            return datetime.fromisoformat(date_str)

        # Start date
        start_date = _str_to_date(match.group(1))

        # End date
        end_date = _str_to_date(match.group(2)) if match.group(2) else None

        return start_date, end_date

    raise ValueError(f"Invalid sys_period format: {sys_period_str}")


# fonction pour trier une liste de batiments et fournir en sortie version la plus récente selon le champ "sys_period"
#  prend en entrée une liste de batiment et retourne pour chaque rnb_id identiques l'élément avec la date la plus récente
def rnb_get_most_recent(liste_batiments: Iterable[dict[str, str]]):

    result = {}

    for bdg in liste_batiments:

        start_date, _ = parse_sys_period(bdg["sys_period"])

        bdg["updated_at"] = start_date

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

        # on veut casser les liens (ajouter à to_remove) si le batiment est inactif (is_active=0)
        # ou a un statut constructionProject ou canceledConstructionProject

        if batiment_rnb["is_active"] == "0" or batiment_rnb["status"] in [
            "constructionProject",
            "canceledConstructionProject",
        ]:
            to_remove.add(batiment_rnb["rnb_id"])

    return to_remove


def remodel_rnb_to_last_changes(batiments_rnb_last_changes):
    """
    ---------------------------------------
    Remodelage des attributs des batiments RNB du fichier rnb_last_changes.csv pour calculer les champs created_at et updated_at :
        ENTREES :
            - batiments_rnb_last_changes : liste des batiments correspondant à "last_changes"
        SORTIES :
            - batiments_rnb_last_changes_remodeled : liste des batis RNB agrémentés des champs created_at et updated_at
    ---------------------------------------
    """

    batiments_rnb_last_changes_remodeled = batiments_rnb_last_changes

    for batiment_rnb in batiments_rnb_last_changes_remodeled:

        start_date, _ = parse_sys_period(batiment_rnb["sys_period"])
        start_date_iso = start_date.isoformat()

        if batiment_rnb["action"] == "create":
            batiment_rnb["created_at"] = start_date_iso
            batiment_rnb["updated_at"] = ""
        else:
            batiment_rnb["created_at"] = ""
            batiment_rnb["updated_at"] = start_date_iso
    return batiments_rnb_last_changes_remodeled


def persist_last_changes(cursor, last_changes, table_creation_date: str):

    print("Remodeling RNB last changes ...")
    last_changes = remodel_rnb_to_last_changes(last_changes)
    print(table_creation_date)

    _insert_last_changes(cursor, last_changes)
    _from_last_changes_to_recserveur(cursor)


def _insert_last_changes(cursor, last_changes):

    # convert last_changes to an in-memory csv
    # then use COPY

    last_changes_csv = io.StringIO()
    fieldnames = last_changes[0].keys()
    writer = csv.DictWriter(last_changes_csv, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(last_changes)
    last_changes_csv.seek(0)

    copy_sql = f"COPY processus_divers.rnb_last_changes ({', '.join(fieldnames)}) FROM STDIN WITH CSV HEADER"
    cursor.copy_expert(copy_sql, last_changes_csv)


def convert_rnb_diff(diff):
    last_changes = rnb_get_most_recent(diff)

    to_remove = calc_to_remove(last_changes)

    return last_changes, to_remove


def persist_to_remove(cursor, to_remove: set):

    # convert to_remove to an in-memory csv
    # then use COPY

    to_remove_csv = io.StringIO()
    writer = csv.writer(to_remove_csv)
    # No header for this one as it's a single column and we don't have fieldnames in DictWriter style here
    # Actually, let's keep it simple or use HEADER to be consistent.
    # _insert_last_changes uses HEADER.
    for rnb_id in to_remove:
        writer.writerow([rnb_id])
    to_remove_csv.seek(0)

    copy_sql = f"COPY processus_divers.rnb_to_remove (rnb_id) FROM STDIN WITH CSV"
    cursor.copy_expert(copy_sql, to_remove_csv)

    # now fille the recserveur table
    _from_toremove_to_recserveur(cursor)


def _from_toremove_to_recserveur(cursor):

    # execute rnb_to_remove_to_delete function
    cursor.execute("SELECT processus_divers.rnb_to_remove_to_delete();")


def _from_last_changes_to_recserveur(cursor):

    # execute rnb_last_changes_to_update function
    cursor.execute("SELECT processus_divers.rnb_last_changes_to_insert();")
    cursor.execute("SELECT processus_divers.rnb_last_changes_to_update();")
    cursor.execute("SELECT processus_divers.rnb_last_changes_to_delete();")

def prepare_recserveur_tables_for_to_remove(cursor):
    commandes_sql = "DROP TABLE IF EXISTS recserveur.delete_batiment_rnb_lien_bdtopo__rnb_deactivation CASCADE;\
                    CREATE TABLE recserveur.delete_batiment_rnb_lien_bdtopo__rnb_deactivation as\
                    select * from pbm.rnb_delete_batiment_rnb_lien_bdtopo__rnb_deactivation;"

    cursor.execute(commandes_sql)

def prepare_recserveur_tables_for_last_changes(cursor):
    commandes_sql = "DROP TABLE IF EXISTS recserveur.delete_batiment_rnb_lien_bdtopo__rnb_demolished CASCADE;\
                    CREATE TABLE recserveur.delete_batiment_rnb_lien_bdtopo__rnb_demolished as\
                    select * from pbm.rnb_delete_batiment_rnb_lien_bdtopo__rnb_demolished;\
                    \
                    DROP TABLE IF EXISTS recserveur.insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage CASCADE;\
                    CREATE TABLE recserveur.insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage as\
                    select * from pbm.rnb_insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage;\
                    \
                    DROP TABLE IF EXISTS recserveur.update_batiment_rnb_lien_bdtopo__moissonnage CASCADE;\
                    CREATE TABLE recserveur.update_batiment_rnb_lien_bdtopo__moissonnage as\
                    select * from pbm.rnb_update_batiment_rnb_lien_bdtopo__moissonnage;"


    cursor.execute(commandes_sql)

def make_link_bdtopo_from_rnb_diff(cursor):
    # execute les fonctions SQL de calcul des liens entre RNB et BDTopo suite à l'import d'un diff RNB

    cursor.execute("SELECT processus_divers.rnb_maj_liens_vers_batiment_car_maj_batiment_rnb();")