import csv
import requests
from collections import defaultdict
from datetime import datetime
import re

"""
---------------------------------------
Récupération en local du fichier CSV de Diff RNB 
à une date donnée en paramètre
---------------------------------------
"""

# date à partir de laquelle on récupère les changements dans le RNB
date_since = "2025-06-15T00:00:00Z"

# URL de l'API RNB qui renvoie un fichier CSV de différentiel
url = "http://rnb-api.beta.gouv.fr/api/alpha/buildings/diff/?since="+date_since

response = requests.get(url)

# Vérifie que la requête a réussi
if response.status_code == 200:
    # Enregistrement du contenu dans un fichier CSV local
    with open("diff_rnb_courant.csv", "wb") as f:
        f.write(response.content)
    print("Fichier CSV téléchargé avec succès.")
else:
    print(f"Erreur lors de la requête : {response.status_code}")

"""
---------------------------------------
Import brut du fichier CSV de Diff RNB
---------------------------------------
"""
# fichier diff à parser
#diff_rnb = 'diff_rnb_courant.csv'
diff_rnb = 'diff_2025-01-01T00_00_44.267000+00_00_2025-02-25T13_53_36.233996+00_00.csv'

# Liste reprenant chaque ligne du csv diff sous forme de dictionnaires
list_batiments_rnb = []

# Parsage du fichier Diff
with open(diff_rnb, mode='r', encoding='utf-8') as diff:
    lecteur_csv = csv.DictReader(diff)
    for ligne in lecteur_csv:
        list_batiments_rnb.append(dict(ligne))

#for batiment_rnb in list_batiments_rnb:
#    print(batiment_rnb)

"""
---------------------------------------
Tri des batiments RNB et export du fichier rnb_last_changes.csv
---------------------------------------
"""

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
        grouped[item['rnb_id']].append(item)

    # Receherche de l'élément avec la date de début la plus récente
    result = []
    for rnb_id, items in grouped.items():
        # On trie les éléments du groupe par date de début décroissante
        sorted_items = sorted(
            items,
            key=lambda x: extract_start_date(x['sys_period']),
            reverse=True
        )
        # On prend le plus récent
        if sorted_items:
            if rnb_id == 'JRWB4B32MJRQ':
                print(sorted_items)
                for item in items:
                    print(extract_start_date(item['sys_period']))

            result.append(sorted_items[0])
    
    return result

# tri des batiments par rnb_id croissant et date la plus récente
list_batiments_rnb_most_recent = rnb_get_most_recent(list_batiments_rnb)
list_batiments_rnb_sorted = sorted(list_batiments_rnb_most_recent, key=lambda x: x["rnb_id"])

# for batiment_rnb in list_batiments_rnb_sorted:
#      print(batiment_rnb)

# Export de la liste triée dans le fichier rnb_last_changes.csv
csv_rnb_last_changes = "rnb_last_changes.csv"

# Écriture des données dans un fichier CSV
with open(csv_rnb_last_changes, mode='w', newline='') as csv_file:
    fieldnames = list_batiments_rnb_sorted[0].keys()
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(list_batiments_rnb_sorted)

"""
---------------------------------------
Tri des batiments RNB du fichier rnb_last_changes.csv pour export dans :
    - link_remove.csv : liste des identifiants RNB à retirer de la table de liens BDTopo
    - link_calculate.csv : liste des identifiants RNB dont le lien doit être créé ou mis à jour avec la BDTopo
---------------------------------------
"""

# on repart du fichier last_changes.csv
csv_rnb_last_changes = "rnb_last_changes.csv"

# Liste reprenant chaque ligne de rnb_last_changes.csv sous forme de dictionnaires
batiments_rnb_last_changes = []

with open(csv_rnb_last_changes, mode='r', encoding='utf-8') as diff:
    lecteur_csv = csv.DictReader(diff)
    for ligne in lecteur_csv:
        batiments_rnb_last_changes.append(dict(ligne))



# création des sets vides
to_remove = set()
to_calculate = set()

for batiment_rnb in batiments_rnb_last_changes:
    if (batiment_rnb['is_active']!="1"):
        # is_active == False, on l'ajoute à to_remove
        to_remove.add(batiment_rnb['rnb_id'])
        continue

    if (batiment_rnb['status']=="constructionProject" or batiment_rnb['status']=="canceledConstructionProject"):
        # valeur du champ status est parmi constructionProject, canceledConstructionProject, on l'ajoute à to_remove
        to_remove.add(batiment_rnb['rnb_id'])
        print("rnb to remove : ", batiment_rnb)
        continue

    # idenfitiant RNB à conserver pour la MAJ du lien BDTopo
    to_calculate.add(batiment_rnb['rnb_id'])

# Export des listes dans les fichier CSV
csv_rnb_to_remove = "link_remove.csv"
csv_rnb_to_calculate = "link_calculate.csv"

with open(csv_rnb_to_remove, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["rnb_id"])
    for id in to_remove:
        writer.writerow([id])

with open(csv_rnb_to_calculate, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["rnb_id"])
    for id in to_calculate:
        writer.writerow([id])

#     print(batiment_rnb)
# for rnb in to_remove:
#     print(rnb)
# for rnb in to_calculate:
#     print(rnb)
