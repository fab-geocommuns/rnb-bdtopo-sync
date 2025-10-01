import csv
import io
import requests
from collections import defaultdict
from datetime import datetime, timedelta
import re
import time

"""
---------------------------------------
Fonction de Récupération en local du fichier CSV de Diff RNB 
à une date donnée en paramètre dans le fichier CSV dont le chemin est fourni en paramètres 
       -- date_since : (format "aaaa-mm-ddThh:mm:ssZ")
       -- file_out : chemin vers le fichier CSV en local
---------------------------------------
"""


def getDiff_RNB_from_date(since: datetime, file_out) -> list:
    # date à partir de laquelle on récupère les changements dans le RNB
    # date_since = "2025-06-15T00:00:00Z"

    # URL de l'API RNB qui renvoie un fichier CSV de différentiel
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


"""
---------------------------------------
Import brut du fichier CSV de Diff RNB
Tri des batiments RNB et export du fichier rnb_last_changes.csv
---------------------------------------
"""


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
    for rnb_id, items in grouped.items():
        # On trie les éléments du groupe par date de début décroissante
        sorted_items = sorted(
            items, key=lambda x: extract_start_date(x["sys_period"]), reverse=True
        )
        # On prend le plus récent
        if sorted_items:

            result.append(sorted_items[0])
        else:
            print("pb sur une recherche de most recent")

    return result


"""
---------------------------------------
Fonction qui parse le fichier CSV fourni en paramètre
et trie les batiments par id_rnb et version unique la plus récente copiés dans un fichier rnb_last_changes.csv au même emplacement
     -ENTREE :
        - file_in : chemin vers le fichier csv à parser
    - SORTIE :
        - list_batiments_rnb_sorted :  liste (liste de dictionnaires) triée des batiments RNB
---------------------------------------
"""


def tri_rnb_last_changes(file_in):
    # fichier diff à parser
    diff_rnb = file_in
    # diff_rnb = 'diff_2025-01-01T00_00_44.267000+00_00_2025-02-25T13_53_36.233996+00_00.csv'

    # Liste reprenant chaque ligne du csv diff sous forme de dictionnaires
    list_batiments_rnb = []

    # Parsage du fichier Diff
    with open(diff_rnb, mode="r", encoding="utf-8") as diff:
        lecteur_csv = csv.DictReader(diff)
        for ligne in lecteur_csv:
            list_batiments_rnb.append(dict(ligne))

    # for batiment_rnb in list_batiments_rnb:
    #    print(batiment_rnb)
    print("nb bati à trier : ", len(list_batiments_rnb))
    # tri des batiments par rnb_id croissant et date la plus récente
    list_batiments_rnb_most_recent = rnb_get_most_recent(list_batiments_rnb)
    print("nb bati plus récents : ", len(list_batiments_rnb_most_recent))
    list_batiments_rnb_sorted = sorted(
        list_batiments_rnb_most_recent, key=lambda x: x["rnb_id"]
    )

    print("nb bati triés : ", len(list_batiments_rnb_sorted))
    return list_batiments_rnb_sorted


"""
---------------------------------------
Fonction qui parse le fichier CSV fourni en paramètre
et trie les batiments par id_rnb et version unique la plus récente copiés dans un fichier rnb_last_changes.csv au même emplacement
     ENTREE :
        - file_in : chemin vers le fichier csv à parser
    SORTIE :
        - csv_rnb_last_changes : chemins vers le fichier csv des batiments RNB triés
---------------------------------------
"""


def tri_rnb_last_changes_csv(file_in):
    # appel de la fonction de tri
    list_batiments_rnb_sorted = tri_rnb_last_changes(file_in)

    # Export de la liste triée dans le fichier rnb_last_changes.csv
    # Enregistrement au même emplacement que le fichier d'entrée

    csv_rnb_last_changes = "rnb_last_changes.csv"

    # Écriture des données dans un fichier CSV
    with open(csv_rnb_last_changes, mode="w", newline="", encoding="utf-8") as csv_file:
        fieldnames = list_batiments_rnb_sorted[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(list_batiments_rnb_sorted)

    return csv_rnb_last_changes


"""
---------------------------------------
Tri des batiments RNB du fichier rnb_last_changes.csv pour export :
    ENTREES : 
        - file_in : chemin vers le fichier csv contenant les "last_changes"
    SORTIES :
        - to_remove: liste (set) des identifiants RNB à retirer de la table de liens BDTopo
        - to_calculate : liste (set) des identifiants RNB dont le lien doit être créé ou mis à jour avec la BDTopo
        - batiments_rnb_last_changes_filtred : liste des batis RNB avec toutes leurs infos sans tous les batis de la liste to_remove
---------------------------------------
"""


# todo : vérifier logique ici
def tri_rnb_to_remove_or_to_calculate(batiments_rnb_last_changes):
    # création des sets vides
    to_remove = set()
    to_calculate = set()
    batiments_rnb_last_changes_to_filter = batiments_rnb_last_changes.copy()

    for batiment_rnb in batiments_rnb_last_changes_to_filter:

        if batiment_rnb["is_active"] == "0":
            # is_active == False, on l'ajoute à to_remove
            to_remove.add(batiment_rnb["rnb_id"])
            batiments_rnb_last_changes.remove(
                batiment_rnb
            )  # on retire le bati de la liste filtree

            continue

        if (
            batiment_rnb["status"] == "constructionProject"
            or batiment_rnb["status"] == "canceledConstructionProject"
        ):
            # valeur du champ status est parmi constructionProject, canceledConstructionProject, on l'ajoute à to_remove
            to_remove.add(batiment_rnb["rnb_id"])
            # print("rnb to remove : ", batiment_rnb)
            batiments_rnb_last_changes.remove(
                batiment_rnb
            )  # on retire le bati de la liste filtree
            continue

        # idenfitiant RNB à conserver pour la MAJ du lien BDTopo
        to_calculate.add(batiment_rnb["rnb_id"])

    # Export des listes dans les fichier CSV
    csv_rnb_to_remove = "link_remove.csv"
    csv_rnb_to_calculate = "link_calculate.csv"

    with open(csv_rnb_to_remove, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["rnb_id"])
        for id in to_remove:
            writer.writerow([id])

    with open(csv_rnb_to_calculate, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["rnb_id"])
        for id in to_calculate:
            writer.writerow([id])

    return batiments_rnb_last_changes, to_remove, to_calculate


"""
---------------------------------------
Remodelage des attributs des batiments RNB du fichier rnb_last_changes.csv pour calculer les champs created_at et updated_at :
    ENTREES : 
        - batiments_rnb_last_changes : liste des batiments correspondant à "last_changes"
    SORTIES :
        - batiments_rnb_last_changes_remodeled : liste des batis RNB agrémentés des champs created_at et updated_at
---------------------------------------
"""
# todo : vérifier logique ici


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


"""
-----------PARTIE TEST --------------------
"""
start = time.time()

# récupération via l'API du fichier de diff
# csv_diff_rnb = "diff_rnb_courant.csv"
# if not getDiff_RNB_from_date("2025-08-15T00:00:00Z",csv_diff_rnb):
#     print("Erreur de récupération du diff. Le programme va s'arrêter")
#     exit()

csv_diff_rnb = "diff_2025-06-01.csv"
csv_rnb_last_changes = tri_rnb_last_changes_csv(csv_diff_rnb)

# Liste reprenant chaque ligne de rnb_last_changes.csv sous forme de dictionnaires
batiments_rnb_last_changes = []
print("lecture CSV terminee")
"""
---------------------------------------
Tri des batiments RNB du fichier rnb_last_changes.csv pour export dans :
    - link_remove.csv : liste des identifiants RNB à retirer de la table de liens BDTopo
    - link_calculate.csv : liste des identifiants RNB dont le lien doit être créé ou mis à jour avec la BDTopo
---------------------------------------
"""

# on repart du fichier last_changes.csv
with open(csv_rnb_last_changes, mode="r", encoding="utf-8") as diff:
    lecteur_csv = csv.DictReader(diff)
    for ligne in lecteur_csv:
        batiments_rnb_last_changes.append(dict(ligne))
print("tri des versions de batis termine")
batiments_rnb_last_changes_filtred, rnb_to_remove, rnb_to_calculate = (
    tri_rnb_to_remove_or_to_calculate(batiments_rnb_last_changes)
)
print("tri en deux listes CSV termine")
# batiments_rnb_last_changes_remodeled = remodel_rnb_to_last_changes(batiments_rnb_last_changes_filtred)


end = time.time()
print(f"Temps d'exécution : {end - start:.4f} secondes")


def sync_rnb(since: datetime):

    # Télécharger le fichier de diff
    rnb_diff = getDiff_RNB_from_date(since)

    pass


if __name__ == "__main__":

    one_week_ago = datetime.now() - timedelta(weeks=1)
    sync_rnb(one_week_ago)
