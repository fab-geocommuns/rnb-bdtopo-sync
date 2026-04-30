-- This file transfers the data from the staging table (staging_batiment_csv) 
-- to the main table (batiment), with necessary transformations and type conversions. 


INSERT INTO public.batiment (
  cleabs, nature, usage_1, usage_2,
  construction_legere, etat_de_l_objet, gcms_detruit,
  gcms_date_creation, gcms_date_modification, gcms_date_destruction,
  date_d_apparition, date_de_confirmation,
  diffusion, source_detaillee, sources, identifiants_sources,
  methode_d_acquisition_planimetrique, precision_planimetrique,
  methode_d_acquisition_altimetrique, precision_altimetrique,
  complement, nombre_de_logements, nombre_d_etages,
  materiaux_des_murs, materiaux_de_la_toiture,
  hauteur, altitude_minimale_sol, altitude_minimale_toit,
  altitude_maximale_toit, altitude_maximale_construction, altitude_maximale_sol,
  nom, commentaire_centralise, commentaire_collecteur,
  origine_du_batiment, source_restitution,
  metadonnees_unification, metadonnees_appariement_ff,
  appariement_fichiers_fonciers, identifiant_parcellaire,
  liens_vers_adresse, liens_vers_evolution,
  gcms_numrec, gcms_territoire, gcms_fingerprint,
  gcvs_nom_lot, exception_legitime,
  geometrie, gcvs_empreinte
)
SELECT
 cleabs,
 nature,
 usage_1,
 usage_2,

  CASE WHEN LOWER(NULLIF(construction_legere, '')) IN ('true','t','1') THEN TRUE
       WHEN LOWER(NULLIF(construction_legere, '')) IN ('false','f','0') THEN FALSE
       ELSE NULL END,

 etat_de_l_objet,
  CASE WHEN LOWER(NULLIF(gcms_detruit, '')) IN ('true','t','1') THEN TRUE
       WHEN LOWER(NULLIF(gcms_detruit, '')) IN ('false','f','0') THEN FALSE
       ELSE NULL END,

  CASE WHEN gcms_date_creation IS NULL OR gcms_date_creation IN ('') THEN NULL
       ELSE gcms_date_creation::timestamp END,
  CASE WHEN gcms_date_modification IS NULL OR gcms_date_modification IN ('') THEN NULL
       ELSE gcms_date_modification::timestamp END,
  CASE WHEN gcms_date_destruction IS NULL OR gcms_date_destruction IN ('') THEN NULL
       ELSE gcms_date_destruction::timestamp END,

  CASE WHEN date_d_apparition IS NULL OR date_d_apparition IN ('') THEN NULL
       ELSE date_d_apparition::date END,
  CASE WHEN date_de_confirmation IS NULL OR date_de_confirmation IN ('') THEN NULL
       ELSE date_de_confirmation::date END,

  CASE WHEN LOWER(NULLIF(diffusion, '')) IN ('true','t','1') THEN TRUE
       WHEN LOWER(NULLIF(diffusion, '')) IN ('false','f','0') THEN FALSE
       ELSE NULL END,

  -- JSONB : NULL / '' / '""' → NULL ; sinon cast si { ... } ou [ ... ] ou "..."
  CASE
    WHEN source_detaillee IS NULL OR source_detaillee IN ('', '""') THEN NULL
    WHEN source_detaillee ~ '^\s*([{\[]|".*")' THEN source_detaillee::jsonb
    ELSE NULL
  END,

  NULLIF(sources, ''),
  NULLIF(identifiants_sources, ''),

  NULLIF(methode_d_acquisition_planimetrique, ''),
  CASE WHEN precision_planimetrique IS NULL OR precision_planimetrique = '' THEN NULL
       ELSE precision_planimetrique::numeric END,
  NULLIF(methode_d_acquisition_altimetrique, ''),
  CASE WHEN precision_altimetrique IS NULL OR precision_altimetrique = '' THEN NULL
       ELSE precision_altimetrique::numeric END,

  CASE
    WHEN complement IS NULL OR complement IN ('', '""') THEN NULL
    WHEN complement ~ '^\s*([{\[]|".*")' THEN complement::jsonb
    ELSE NULL
  END,

  CASE WHEN nombre_de_logements IS NULL OR nombre_de_logements = '' THEN NULL
       ELSE nombre_de_logements::int END,
  CASE WHEN nombre_d_etages IS NULL OR nombre_d_etages = '' THEN NULL
       ELSE nombre_d_etages::int END,

  NULLIF(materiaux_des_murs, ''),
  NULLIF(materiaux_de_la_toiture, ''),

  CASE WHEN hauteur IS NULL OR hauteur = '' THEN NULL ELSE hauteur::numeric END,
  CASE WHEN altitude_minimale_sol IS NULL OR altitude_minimale_sol = '' THEN NULL ELSE altitude_minimale_sol::numeric END,
  CASE WHEN altitude_minimale_toit IS NULL OR altitude_minimale_toit = '' THEN NULL ELSE altitude_minimale_toit::numeric END,
  CASE WHEN altitude_maximale_toit IS NULL OR altitude_maximale_toit = '' THEN NULL ELSE altitude_maximale_toit::numeric END,
  CASE WHEN altitude_maximale_construction IS NULL OR altitude_maximale_construction = '' THEN NULL ELSE altitude_maximale_construction::numeric END,
  CASE WHEN altitude_maximale_sol IS NULL OR altitude_maximale_sol = '' THEN NULL ELSE altitude_maximale_sol::numeric END,

  NULLIF(nom, ''),
  NULLIF(commentaire_centralise, ''),
  NULLIF(commentaire_collecteur, ''),
  NULLIF(origine_du_batiment, ''),
  NULLIF(source_restitution, ''),

  -- varchar (pas JSON) → on met NULL si vide
  CASE WHEN metadonnees_unification IS NULL OR metadonnees_unification IN ('', '""') THEN NULL
       ELSE metadonnees_unification END,

  CASE
    WHEN metadonnees_appariement_ff IS NULL OR metadonnees_appariement_ff IN ('', '""') THEN NULL
    WHEN metadonnees_appariement_ff ~ '^\s*([{\[]|".*")' THEN metadonnees_appariement_ff::jsonb
    ELSE NULL
  END,

  NULLIF(appariement_fichiers_fonciers, ''),
  NULLIF(identifiant_parcellaire, ''),
  NULLIF(liens_vers_adresse, ''),
  NULLIF(liens_vers_evolution, ''),

  CASE WHEN gcms_numrec IS NULL OR gcms_numrec = '' THEN NULL ELSE gcms_numrec::int END,
  NULLIF(gcms_territoire, ''),
  NULLIF(gcms_fingerprint, ''),
  NULLIF(gcvs_nom_lot, ''),
  NULLIF(exception_legitime, ''),

  CASE WHEN geometrie IS NULL OR geometrie = '' THEN NULL
       ELSE ST_GeomFromText(geometrie) END,
  CASE WHEN gcvs_empreinte IS NULL OR gcvs_empreinte = '' THEN NULL
       ELSE ST_GeomFromText(gcvs_empreinte) END
FROM staging_batiment_csv;