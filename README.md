# Présentation

Ce script est utilisé par l'IGN pour lister les identifiants RNB qui sont à tenter d'apparier avec la BD Topo et ceux qui sont éventuellement à retirer.

# Données d'entrée

La donnée d'entrée du script est [le fichier de diff](https://rnb-fr.gitbook.io/documentation/api-et-outils/api-batiments/differentiel-entre-deux-dates) fourni par le RNB.

# Données de sortie

La donnée de sortie est composée de : 

- Pour tenir à jour le stock RNB entretenu par l'IGN :
  - une liste des bâtiments RNB (avec l'ensemble de leurs attributs) à créer ou mettre à jour, mise à disposition dans un fichier CSV `rnb_last_changes.csv`
- Pour tenir à jour la table de correspondance RNB x BD Topo :
  - une liste d'identifiants RNB à retirer de la table de correspondance RNB x BD Topo, mise à disposition dans un fichier CSV `link_remove.csv`
  - une liste d'identifiants RNB où l'appariement RNB x BD Topo doit être (re)fait, mise à disposition dans un fichier CSV `link_calculate.csv`

Les fichiers `stock_create.csv` et `stock_update.csv` ont le même format que le fichier de diff RNB mais ne contiennent qu'une seule ligne par bâtiment RNB (la plus récente).
Les fichiers `link_remove.csv` et `link_calculate.csv` sont composés d'une seule colonne `rnb_id`.

# Constitution des fichiers CSV

## `rnb_last_changes.csv`

On part du fichier de diff RNB.

1. On trie l'ensemble des lignes par `rnb_id` puis pour chaque groupe d'ID RNB, par `updated_at`
2. Pour chaque groupe d'ID RNB, on ne garde que la dernière ligne de chaque bâtiment (la plus récente)
3. On sauvegarde le fichier CSV

On importera le fichier `rnb_last_changes.csv` en faisant un `upsert` pour chaque ligne.

## `link_remove.csv`

On part du fichier `rnb_last_changes.csv` fabriqué à l'étape précédente.

1. Pour chaque ligne on vérifie si :
   a. le champ `is_active == False`
   b. ou la valeur du champ `status` est parmi `constructionProject`, `canceledConstructionProject`, `demolished`
2. On conserve les lignes répondant à une de ces conditions
3. On conserve uniquement le champ `rnb_id` de chaque ligne
4. On enregistre cette liste d'identifiants RNB dans le fichier CSV `link_remove.csv`


