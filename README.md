# Présentation

Ce script est utilisé par l'IGN pour lister les identifiants RNB qui sont à tenter d'apparier avec la BD Topo et ceux qui sont éventuellement à retirer.

# Données d'entrée

La donnée d'entrée du script est [le fichier de diff](https://rnb-fr.gitbook.io/documentation/api-et-outils/api-batiments/differentiel-entre-deux-dates) fourni par le RNB.

# Données de sortie

La donnée de sortie est composée de : 

- Pour tenir à jour le stock RNB entretenu par l'IGN :
  - une liste des bâtiments RNB (avec l'ensemble de leurs attributs) à créer, mise à disposition dans un fichier CSV `stock_create.csv`
  - une liste des bâtiments RNB (avec l'ensemble de leurs attributs) à mettre à jour, mise à disposition dans un fichier CSV `stock_update.csv`
- Pour tenir à jour la table de correspondance RNB x BD Topo :
  - une liste d'identifiants RNB à retirer de la table de correspondance RNB x BD Topo, mise à disposition dans un fichier CSV `link_remove.csv`
  - une liste d'identifiants RNB où l'appariement RNB x BD Topo doit être (re)fait, mise à disposition dans un fichier CSV `link_calculate.csv`

Les fichiers `stock_create.csv` et `stock_update.csv` ont le même format que le fichier de diff RNB mais ne contiennent qu'une seule ligne par bâtiment RNB (la plus récente).
Les fichiers `link_remove.csv` et `link_calculate.csv` sont composés d'une seule colonne `rnb_id`.

