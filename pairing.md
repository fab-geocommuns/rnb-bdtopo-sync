# Processus d'appariement RNB x BD TOPO

Ce document décrit le processus d'appariement entre les bâtiments du Référentiel National des Bâtiments (RNB) et ceux de la BD TOPO de l'IGN.

## Vue d'ensemble

L'appariement RNB x BD TOPO consiste à établir des liens (`liens_vers_batiment`) entre les identifiants des bâtiments RNB (`identifiant_rnb`) et les identifiants des bâtiments BD TOPO (`cleabs`). Ce processus se déroule en plusieurs étapes successives, chacune traitant des cas géométriques et sémantiques spécifiques.

## Tables de travail

Le processus utilise quatre tables temporaires principales :

- `processus_divers.rnb_batiments_rnb_restant_creation` : Bâtiments RNB non encore appariés
- `processus_divers.rnb_batiments_rnb_traites_creation` : Bâtiments RNB déjà appariés
- `processus_divers.rnb_batiments_bduni_restant_creation` : Bâtiments BD TOPO non encore appariés
- `processus_divers.rnb_batiments_bduni_traites_creation` : Bâtiments BD TOPO déjà appariés

À chaque étape, les bâtiments appariés sont transférés des tables "restant" vers les tables "traités".

## Étape de préparation

**Fonction SQL :** `processus_divers.rnb_preparation_creation()` ou `processus_divers.rnb_maj_liens_vers_batiment_car_maj_batiment_rnb()`

### Objectif
Préparer les tables de travail avec les bâtiments RNB et BD TOPO à apparier.

### Processus
1. **Récupération des bâtiments RNB** :
   - Récupère les bâtiments RNB créés ou modifiés depuis la dernière synchronisation
   - Source : tables `insert_batiment_rnb_lien_bdtopo__creationerentiel` et `update_batiment_rnb_lien_bdtopo__creationerentiel`

2. **Création d'une zone tampon** :
   - Applique un buffer de 100m autour des géométries RNB
   - Fusionne ces buffers pour créer une zone de recherche optimisée

3. **Sélection des bâtiments BD TOPO candidats** :
   - Récupère uniquement les bâtiments BD TOPO qui intersectent la zone tampon
   - Condition : `NOT gcms_detruit` (bâtiments non détruits)
   - Cette optimisation réduit significativement le volume de données à traiter

4. **Initialisation des tables de travail** :
   - Table `rnb_batiments_rnb_restant_creation` : contient les bâtiments RNB à apparier
   - Table `rnb_batiments_bduni_restant_creation` : contient les bâtiments BD TOPO candidats
   - Tables vides `rnb_batiments_rnb_traites_creation` et `rnb_batiments_bduni_traites_creation` pour stocker les résultats

## Étape 1 : Identification des bâtiments non appariables

**Fonction SQL :** `processus_divers.rnb_batiment_non_apparie_creation()`
**Traitement :** `'Bâtiment non apparié, à plus de 20m d'un autre bâtiment'`

### Objectif
Identifier les bâtiments qui ne peuvent pas être appariés car ils sont trop éloignés géographiquement.

### Critères
- Bâtiments RNB à plus de 20m de tout bâtiment BD TOPO
- Bâtiments BD TOPO à plus de 20m de tout bâtiment RNB

### Méthode
1. Création de clusters de bâtiments par union géométrique
2. Utilisation de `ST_DWITHIN(geometrie, 20)` pour identifier les proximités
3. Exclusion des bâtiments isolés

### Résultat
- Ces bâtiments sont marqués comme "non appariables"
- Ils sont transférés dans les tables "traités" avec un champ `liens_vers_batiment` vide

## Étape 2 : Appariement sémantique

**Fonction SQL :** `processus_divers.rnb_croisement_semantique_creation()`
**Traitement :** `'Croisement sémantique'`

### Objectif
Établir des liens directs basés sur les identifiants externes stockés dans le RNB.

### Critères
- Le champ `informations_rnb::jsonb->>'ext_ids'` du bâtiment RNB contient un `id` correspondant à un `cleabs` BD TOPO

### Méthode
```sql
WHERE elem->>'id' IN (
    SELECT cleabs
    FROM processus_divers.rnb_batiments_bduni_restant_creation
)
```

### Résultat
- Appariement direct et fiable basé sur les métadonnées
- Priorité maximale car il s'agit de liens explicites

## Étape 3 : Bâtiments semblables (même nombre de points)

**Fonction SQL :** `processus_divers.rnb_batiment_semblable_creation()`
**Traitement :** `'Batiments semblables'`

### Objectif
Identifier les bâtiments quasi-identiques en forme et en taille.

### Critères géométriques
- **Intersection significative** : `floor(ST_Area(ST_Intersection(...))) > 3` m²
- **Aires similaires** : `abs(floor(st_area(bdtopo)) - floor(st_area(rnb))) < 3` m²
- **Périmètres similaires** : `abs(floor(st_perimeter(bdtopo)) - floor(st_perimeter(rnb))) < 3` m
- **Même complexité** : `ST_NPoints(bdtopo) = ST_NPoints(rnb)`

### Méthode
- Comparaison géométrique stricte
- Tolérance de 3m² sur les surfaces et 3m sur les périmètres
- Vérification du nombre de points identique (géométries comparables)

### Résultat
- Appariement fiable pour les bâtiments dont la géométrie est très proche

## Étape 4 : Bâtiments semblables (nombre de points différent)

**Fonction SQL :** `processus_divers.rnb_batiment_semblable_nbp_creation()`
**Traitement :** `'Batiments semblables nbpt diff'`

### Objectif
Apparier les bâtiments de forme et taille similaires mais avec des géométries simplifiées/détaillées différemment.

### Critères géométriques
- **Intersection significative** : `floor(ST_Area(ST_Intersection(...))) > 3` m²
- **Aires similaires** : `abs(floor(st_area(bdtopo)) - floor(st_area(rnb))) < 3` m²
- **Périmètres similaires** : `abs(floor(st_perimeter(bdtopo)) - floor(st_perimeter(rnb))) < 3` m
- **Complexité différente** : `ST_NPoints(bdtopo) != ST_NPoints(rnb)`

### Différence avec l'étape 3
Cette étape traite les cas où un bâtiment peut être représenté avec plus ou moins de détails géométriques (simplification ou complexification du contour).

## Étape 5 : Bâtiments RNB fusionnés semblables

**Fonction SQL :** `processus_divers.rnb_batiment_fusionnee_semblable_creation()`
**Traitement :** `'Batiments RNB dissous semblables'`

### Objectif
Traiter les cas où plusieurs bâtiments RNB adjacents correspondent à un seul bâtiment BD TOPO.

### Méthode
1. **Fusion des géométries RNB** :
   ```sql
   ST_Union(geometrie_enveloppe) -- Fusionne les bâtiments RNB qui s'intersectent
   ```

2. **Comparaison avec BD TOPO** :
   - Intersection avec le bâtiment fusionné : `floor(ST_Area(ST_Intersection(...))) > 3` m²
   - Aires similaires : `abs(floor(area(bdtopo)) - floor(area(rnb_fusionné))) < 3` m²
   - Périmètres similaires : `abs(floor(perimeter(bdtopo)) - floor(perimeter(rnb_fusionné))) < 3` m

### Cas d'usage typique
- Division cadastrale : plusieurs parcelles RNB pour un seul bâtiment physique
- Évolution du découpage administratif

## Étape 6 : Bâtiments inclus

**Fonction SQL :** `processus_divers.rnb_batiment_inclus_creation()`
**Traitement :** `'Batiments inclus'`

### Objectif
Identifier les bâtiments RNB complètement contenus dans un bâtiment BD TOPO.

### Critères
```sql
ST_Contains(bdtopo.geometrie, ST_Buffer(rnb.geometrie_enveloppe, -0.5))
```

### Méthode
- Utilisation d'un buffer négatif de -0.5m pour gérer les imprécisions géométriques
- Le bâtiment RNB doit être entièrement à l'intérieur du bâtiment BD TOPO

### Cas d'usage
- Bâtiments intérieurs (locaux techniques, commerces dans centres commerciaux)
- Différence de niveau de détail entre les deux bases

## Étape 7 : Détection des balcons et petites annexes

**Fonction SQL :** `processus_divers.rnb_batiment_balcon_et_maj_creation()`
**Traitement :** `'Batiments BDTOPO balcon'`

### Objectif
Filtrer les petites structures qui ne sont pas des bâtiments principaux.

### Critères
- **Surface BD TOPO** : `floor(st_area(bdtopo)) < 25` m²
- **Surface relative** : `floor(st_area(bdtopo)) < floor(st_area(rnb.geometrie_enveloppe))`
- **Intersection** : `st_intersects(rnb.geometrie_enveloppe, bdtopo.geometrie)`
- **Intersection minimale** : `floor(st_area(st_intersection(...))) < 3` m²

### Méthode
- Identifie les petites structures (balcons, terrasses, auvents)
- Compare avec les bâtiments RNB traités ET restants
- Ces structures sont liées au bâtiment principal mais marquées comme "balcon"

## Étapes complémentaires

### Bâtiments égaux
**Fonction SQL :** `processus_divers.rnb_batiment_egaux_creation()`
**Traitement :** `'Bâtiments égaux'`

- Utilise `st_equals(bdtopo.geometrie, rnb.geometrie_enveloppe)`
- Identifie les géométries strictement identiques

### Bâtiments identiques
**Fonction SQL :** `processus_divers.rnb_batiment_identique_creation()`
**Traitement :** `'Batiments identiques'`

- Critères :
  - Intersection complète : `floor(st_area(intersection)) = floor(st_area(bdtopo))`
  - ET `floor(st_area(intersection)) = floor(st_area(rnb))`

### Bâtiments ponctuels
**Fonction SQL :** `processus_divers.rnb_ponctuel_reste()`
**Traitement :** `'Bâtiments ponctuels'`

- Cas où le bâtiment BD TOPO intersecte la géométrie ponctuelle RNB
- Utilisé en dernier recours pour les bâtiments RNB représentés par un point

## Mise à jour des tables de travail

**Fonction SQL :** `processus_divers.rnb_maj_des_tables_de_travail_creation()`

### Processus
Après chaque étape d'appariement :

1. **Transfert des bâtiments BD TOPO appariés** :
   - De `rnb_batiments_bduni_restant_creation`
   - Vers `rnb_batiments_bduni_traites_creation`
   - Avec les champs `traitement` et `identifiant_rnb`

2. **Transfert des bâtiments RNB appariés** :
   - De `rnb_batiments_rnb_restant_creation`
   - Vers `rnb_batiments_rnb_traites_creation`
   - Avec les champs `traitement` et `liens_vers_batiment`

3. **Suppression des bâtiments traités** :
   - Nettoyage des tables "restant" pour éviter les doublons

## Résultats finaux

### Table de résultats
La table finale `public.batiment_rnb_lien_bdtopo` contient :

- **cleabs** : Identifiant unique du bâtiment (auto-généré)
- **identifiant_rnb** : Identifiant RNB du bâtiment
- **geometrie** : Géométrie ponctuelle du bâtiment RNB
- **geometrie_enveloppe** : Emprise géométrique (multipolygone) du bâtiment RNB
- **informations_rnb** : Métadonnées JSON (ext_ids, created_at, updated_at, identifiants_ban)
- **liens_vers_batiment** : Liste des `cleabs` BD TOPO appariés (séparés par `/`)
- **traitement** : Type d'appariement appliqué
- **status** : Statut du bâtiment RNB (constructed, demolished, etc.)
- **event_type** : Type d'événement (creation, reactivation, etc.)

### Format des liens
Les liens sont stockés sous forme de chaîne de caractères avec le format :
```
cleabs1/cleabs2/cleabs3
```

### Statistiques
La table `processus_divers.stat_rnb` conserve les statistiques :
- `nb_de_insert` : Nombre de bâtiments RNB créés
- `nb_de_delete` : Nombre de bâtiments RNB supprimés
- `nb_de_update` : Nombre de bâtiments RNB mis à jour
- `nb_batiment_bdu_apparies` : Nombre de bâtiments BD TOPO appariés

## Gestion des mises à jour

### Mise à jour suite à modification de bâtiments RNB
**Fonction SQL :** `processus_divers.rnb_maj_liens_vers_batiment_car_maj_batiment_rnb()`

Lorsque des bâtiments RNB sont créés ou modifiés :
1. Récupération des bâtiments RNB modifiés dans les 2 derniers jours
2. Création d'un buffer de 100m autour de ces bâtiments
3. Sélection des bâtiments BD TOPO dans cette zone
4. Ré-exécution de toutes les étapes d'appariement
5. Mise à jour de la table `update_batiment_rnb_lien_bdtopo__batiment_rnb_creation`

### Mise à jour suite à création de bâtiments BD TOPO
**Fonction SQL :** `processus_divers.rnb_maj_liens_vers_batiment_car_creation_batiment_bdtopo()`

Lorsque des bâtiments BD TOPO sont créés :
1. Récupération des bâtiments BD TOPO créés dans les 8 derniers jours
2. Création d'un buffer de 100m autour de ces bâtiments
3. Sélection des bâtiments RNB dans cette zone
4. Ré-exécution de toutes les étapes d'appariement
5. Mise à jour de la table `update_batiment_rnb_lien_bdtopo__batiment_bdtopo_creation`

### Mise à jour suite à suppression de bâtiments BD TOPO
**Fonction SQL :** `processus_divers.rnb_maj_liens_vers_batiment_car_suppr_batiment_bdtopo()`

Lorsque des bâtiments BD TOPO sont supprimés :
1. Récupération des bâtiments BD TOPO supprimés dans les 8 derniers jours
2. Identification des bâtiments RNB liés à ces bâtiments supprimés
3. Éclatement des `liens_vers_batiment` pour retirer les références supprimées
4. Mise à jour de la table `update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr`

## Ordre d'exécution des étapes

L'ordre est important car chaque étape traite les bâtiments restants :

1. **Préparation** : Initialisation des tables de travail
2. **Bâtiments non appariables** : Isolation des cas impossibles
3. **Croisement sémantique** : Appariement le plus fiable
4. **Bâtiments semblables** : Géométries très proches (même nombre de points)
5. **Bâtiments semblables (nbpt diff)** : Géométries proches (nombre de points différent)
6. **Bâtiments fusionnés** : Regroupements de bâtiments RNB
7. **Bâtiments inclus** : Bâtiments contenus
8. **Balcons et annexes** : Petites structures

## Points d'attention

### Tolérances géométriques
- **Surfaces** : 3 m² de tolérance
- **Périmètres** : 3 m de tolérance
- **Distance maximale** : 20 m pour considérer un appariement possible
- **Buffer d'inclusion** : -0.5 m pour gérer les imprécisions

### Performances
- Utilisation systématique d'index spatiaux (GIST)
- Buffers de pré-sélection pour limiter les comparaisons
- Traitement par lots des bâtiments modifiés

### Gestion des cas complexes
- **1 RNB → N BD TOPO** : Un bâtiment RNB peut être lié à plusieurs BD TOPO
- **N RNB → 1 BD TOPO** : Plusieurs RNB peuvent pointer vers le même BD TOPO (fusion)
- **Géométries multiples** : Utilisation de `ST_Multi()` pour normaliser les géométries

## Fichiers et scripts Python

### Script principal
- **run.py** : Script d'exécution principal
  - `sync_rnb(since: datetime)` : Synchronisation depuis une date
  - `sync_rnb_from_file(filename: str)` : Synchronisation depuis un fichier

### Module RNB
- **rnb.py** : Gestion des données RNB
  - `getDiff_RNB_from_date()` : Récupération du différentiel depuis l'API
  - `getDiff_RNB_from_file()` : Lecture depuis un fichier local
  - `rnb_get_most_recent()` : Conservation de la version la plus récente par bâtiment
  - `calc_to_remove()` : Calcul des bâtiments à retirer
  - `persist_last_changes()` : Insertion des changements en base
  - `persist_to_remove()` : Insertion des bâtiments à supprimer

### Module d'appariement
- **pairing.py** : Exécution du processus d'appariement
  - `run_pairing_after_rnb_update()` : Lance l'appariement après mise à jour RNB

## Conclusion

Le processus d'appariement RNB x BD TOPO est un processus itératif et hiérarchisé qui traite successivement différents cas géométriques et sémantiques. L'ordre d'exécution des étapes est optimisé pour traiter en priorité les cas les plus fiables (sémantique) puis les cas géométriques du plus contraint au moins contraint.

Les tolérances géométriques permettent de gérer les imprécisions inhérentes aux différentes sources de données tout en maintenant une qualité d'appariement élevée.
