import csv
import io
import re
import requests
from collections import defaultdict
from datetime import datetime


def getDiff_RNB_from_date(since: datetime) -> list:

    url = (
        "http://rnb-api.beta.gouv.fr/api/alpha/buildings/diff/?since="
        + since.isoformat()
    )

    # On télécharge le fichier de diff du RNB
    response = requests.get(url)
    response.raise_for_status()

    # On garde le contenu du CSV en mémoire.
    # Pas besoin de l'écrire dans un fichier
    csv_file = io.StringIO(response.text)
    reader = csv.reader(csv_file)

    # On renvoit le contenu du CSV sous forme de liste
    return list(reader)


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
def rnb_get_most_recent(liste_batiments):
    # Groupement des éléments par rnb_id
    grouped = defaultdict(list)
    for item in liste_batiments:
        grouped[item["rnb_id"]].append(item)

    # Receherche de l'élément avec la date de début la plus récente
    result = []
    for _, items in grouped.items():
        # On trie les éléments du groupe par date de début décroissante
        sorted_items = sorted(
            items, key=lambda x: extract_start_date(x["sys_period"]), reverse=True
        )
        # On prend le plus récent
        if sorted_items:
            result.append(sorted_items[0])
        else:
            raise ValueError("Aucun élément trouvé pour le groupe donné.")

    return result


def dispatch_rows(rnb_diff) -> tuple[set, set]:

    to_remove = set()
    to_calculate = set()

    for batiment_rnb in rnb_diff:

        # on veut casser les liens (ajouter à to_remove) si le batiment est inactif (is_active=0)
        # ou a un statut constructionProject ou canceledConstructionProject
        # sinon, on veut créer ou mettre à jour le lien (ajouter à to_calculate)

        if batiment_rnb["is_active"] == "0" or batiment_rnb["status"] in [
            "constructionProject",
            "canceledConstructionProject",
        ]:
            to_remove.add(batiment_rnb["rnb_id"])
        else:
            to_calculate.add(batiment_rnb["rnb_id"])

    return to_remove, to_calculate


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
