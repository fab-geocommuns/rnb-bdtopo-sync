import csv

"""
---------------------------------------
Import brut du fichier CSV de Diff RNB
---------------------------------------
"""
# fichier diff à parser
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

# tri des batiments par rnb_id croissant
list_batiments_rnb_sorted = sorted(list_batiments_rnb, key=lambda x: x["rnb_id"])

# for batiment_rnb in list_batiments_rnb_sorted:
#     print(batiment_rnb)

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
for rnb in to_calculate:
    print(rnb)
