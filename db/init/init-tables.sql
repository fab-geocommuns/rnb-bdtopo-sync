 
DROP TABLE IF EXISTS public.batiment_rnb_lien_bdtopo;

CREATE TABLE public.batiment_rnb_lien_bdtopo (
	cleabs varchar(24) NOT NULL,
	identifiant_rnb varchar NULL,
	liens_vers_batiment varchar NULL,
	informations_rnb varchar NULL,
	geometrie_enveloppe public.geometry(multipolygon) NULL,
	nom varchar NULL,
	commentaire_centralise varchar NULL,
	commentaire_collecteur varchar NULL,
	gcms_detruit bool DEFAULT false NOT NULL,
	gcms_date_creation timestamp NULL,
	gcms_date_modification timestamp NULL,
	gcms_date_destruction timestamp NULL,
	diffusion bool NOT NULL,
	gcms_numrec int4 NOT NULL,
	gcms_territoire varchar NOT NULL,
	gcms_fingerprint varchar NULL,
	exception_legitime varchar NULL,
	gcvs_empreinte public.geometry(linestring) NULL,
	traitement varchar NULL,
	gcvs_nom_lot varchar NULL,
	geometrie public.geometry(point) NOT NULL,
	status varchar NULL,
	event_type varchar NULL,
	parent_buildings jsonb NULL,
	CONSTRAINT batiment_rnb_lien_bdtopo_pkey PRIMARY KEY (cleabs),
	CONSTRAINT enforce_srid_empreinte CHECK ((st_srid(gcvs_empreinte) = 0)),
	CONSTRAINT enforce_srid_geometrie CHECK ((st_srid(geometrie) = 0))
);
CREATE INDEX batiment_rnb_lien_bdtopo_gcms_numrec_idx ON public.batiment_rnb_lien_bdtopo USING btree (gcms_numrec);
CREATE INDEX batiment_rnb_lien_bdtopo_gcms_territoire_idx ON public.batiment_rnb_lien_bdtopo USING btree (gcms_territoire);
CREATE INDEX batiment_rnb_lien_bdtopo_gcvs_empreinte_idx ON public.batiment_rnb_lien_bdtopo USING gist (gcvs_empreinte);
CREATE INDEX batiment_rnb_lien_bdtopo_geometrie_idx ON public.batiment_rnb_lien_bdtopo USING gist (geometrie);
CREATE UNIQUE INDEX batiment_rnb_lien_bdtopo_identifiant_rnb_gcms_numrec_idx ON public.batiment_rnb_lien_bdtopo USING btree (identifiant_rnb, gcms_numrec);
CREATE UNIQUE INDEX batiment_rnb_lien_bdtopo_identifiant_rnb_idx ON public.batiment_rnb_lien_bdtopo USING btree (identifiant_rnb);
CREATE INDEX batiment_rnb_lien_bdtopo_liens_vers_batiment_idx ON public.batiment_rnb_lien_bdtopo USING btree (liens_vers_batiment);



DROP TABLE IF EXISTS public.batiment;

CREATE TABLE public.batiment (
	cleabs varchar(24) NOT NULL,
	nature varchar NULL,
	usage_1 varchar NULL,
	usage_2 varchar NULL,
	construction_legere bool NULL,
	etat_de_l_objet varchar DEFAULT 'En service'::character varying NULL,
	gcms_detruit bool NOT NULL,
	gcms_date_creation timestamp NULL,
	gcms_date_modification timestamp NULL,
	gcms_date_destruction timestamp NULL,
	date_d_apparition date NULL,
	date_de_confirmation date NULL,
	diffusion bool DEFAULT true NOT NULL,
	source_detaillee jsonb NULL,
	sources varchar NULL,
	identifiants_sources varchar NULL,
	methode_d_acquisition_planimetrique varchar DEFAULT 'Fichier numérique non métrique'::character varying NULL,
	precision_planimetrique numeric(5, 1) NULL,
	methode_d_acquisition_altimetrique varchar DEFAULT 'Pas de Z'::character varying NULL,
	precision_altimetrique numeric(5, 1) NULL,
	complement jsonb NULL,
	nombre_de_logements int4 NULL,
	nombre_d_etages int4 NULL,
	materiaux_des_murs varchar NULL,
	materiaux_de_la_toiture varchar NULL,
	hauteur numeric(6, 2) NULL,
	altitude_minimale_sol numeric(7, 2) NULL,
	altitude_minimale_toit numeric(7, 2) NULL,
	altitude_maximale_toit numeric(7, 2) NULL,
	altitude_maximale_construction numeric(7, 2) NULL,
	altitude_maximale_sol numeric(7, 2) NULL,
	nom varchar NULL,
	commentaire_centralise varchar NULL,
	commentaire_collecteur varchar NULL,
	origine_du_batiment varchar NULL,
	source_restitution varchar NULL,
	metadonnees_unification varchar NULL,
	metadonnees_appariement_ff jsonb NULL,
	appariement_fichiers_fonciers varchar NULL,
	identifiant_parcellaire varchar NULL,
	liens_vers_adresse varchar NULL,
	liens_vers_evolution varchar NULL,
	gcms_numrec int4 NOT NULL,
	gcms_territoire varchar NOT NULL,
	gcms_fingerprint varchar NULL,
	gcvs_nom_lot varchar NULL,
	exception_legitime varchar NULL,
	geometrie public.geometry(multipolygonz) NOT NULL,
	gcvs_empreinte public.geometry(linestring) NULL,
	CONSTRAINT batiment_pkey PRIMARY KEY (cleabs),
	CONSTRAINT enforce_srid_empreinte CHECK ((st_srid(gcvs_empreinte) = 0)),
	CONSTRAINT enforce_srid_geometrie CHECK ((st_srid(geometrie) = 0))
	
);
CREATE INDEX batiment_gcms_numrec_idx ON public.batiment USING btree (gcms_numrec);
CREATE INDEX batiment_gcms_territoire_idx ON public.batiment USING btree (gcms_territoire);
CREATE INDEX batiment_gcvs_empreinte_idx ON public.batiment USING gist (gcvs_empreinte);
CREATE INDEX batiment_geometrie_idx ON public.batiment USING gist (geometrie);

-- creation de la table de stagging pour import du CSV--

DROP TABLE IF EXISTS staging_batiment_csv;

CREATE TABLE staging_batiment_csv (
  cleabs text,
  nature text,
  usage_1 text,
  usage_2 text,
  construction_legere text,
  etat_de_l_objet text,
  gcms_detruit text,
  gcms_date_creation text,
  gcms_date_modification text,
  gcms_date_destruction text,
  date_d_apparition text,
  date_de_confirmation text,
  diffusion text,
  source_detaillee text,
  sources text,
  identifiants_sources text,
  methode_d_acquisition_planimetrique text,
  precision_planimetrique text,
  methode_d_acquisition_altimetrique text,
  precision_altimetrique text,
  complement text,
  nombre_de_logements text,
  nombre_d_etages text,
  materiaux_des_murs text,
  materiaux_de_la_toiture text,
  hauteur text,
  altitude_minimale_sol text,
  altitude_minimale_toit text,
  altitude_maximale_toit text,
  altitude_maximale_construction text,
  altitude_maximale_sol text,
  nom text,
  commentaire_centralise text,
  commentaire_collecteur text,
  origine_du_batiment text,
  source_restitution text,
  metadonnees_unification text,
  metadonnees_appariement_ff text,
  appariement_fichiers_fonciers text,
  identifiant_parcellaire text,
  liens_vers_adresse text,
  liens_vers_evolution text,
  gcms_numrec text,
  gcms_territoire text,
  gcms_fingerprint text,
  gcvs_nom_lot text,
  exception_legitime text,
  geometrie text,
  gcvs_empreinte text
);


-- COPY staging_batiment_csv
-- FROM '/docker-entrypoint-initdb.d/batiment_bdtopo_candidats.csv'
-- DELIMITER ','
-- CSV HEADER;


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

DROP TABLE IF EXISTS public.gcms_territoire;

CREATE TABLE public.gcms_territoire (
	nom text NULL,
	code text NOT NULL,
	dep text NULL,
	srid int4 NULL,
	geometrie public.geometry(polygon) NULL,
	CONSTRAINT gcms_territoire_pkey PRIMARY KEY (code)
);
CREATE INDEX gcms_territoire_geometrie ON public.gcms_territoire USING gist (geometrie);
INSERT INTO public.gcms_territoire (nom,code,dep,srid,geometrie) VALUES
	 ('Guyane','GUF','973',2972,'POLYGON ((-100000 700000, 500000 700000, 500000 100000, -100000 100000, -100000 700000))'),
	 ('la Réunion','REU','974',2975,'POLYGON ((300000 7620000, 300000 7710000, 390000 7710000, 390000 7620000, 300000 7620000))'),
	 ('Saint-Pierre-et-Miquelon','SPM','975',4467,'POLYGON ((530000 5160000, 530000 5240000, 580000 5240000, 580000 5160000, 530000 5160000))'),
	 ('Mayotte','MYT','976',4471,'POLYGON ((490000 8550000, 490000 8620000, 540000 8620000, 540000 8550000, 490000 8550000))'),
	 ('Guadeloupe','GLP','971',5490,'POLYGON ((470000 1730000, 470000 2020000, 730000 2020000, 730000 1730000, 470000 1730000))'),
	 ('Martinique','MTQ','972',5490,'POLYGON ((680000 1580000, 680000 1660000, 750000 1660000, 750000 1580000, 680000 1580000))'),
	 ('Bassas da India','BAS','984',7074,'POLYGON ((550000 7600000, 550000 7650000, 600000 7650000, 600000 7600000, 550000 7600000))'),
	 ('Île Europa','EUR','984',7074,'POLYGON ((620000 7500000, 620000 7550000, 670000 7550000, 670000 7500000, 620000 7500000))'),
	 ('Îles Glorieuses','GLO','984',32738,'POLYGON ((730000 8700000, 730000 8750000, 780000 8750000, 780000 8700000, 730000 8700000))'),
	 ('Île Juan de Nova','JUA','984',32738,'POLYGON ((230000 8090000, 230000 8140000, 280000 8140000, 280000 8090000, 230000 8090000))');
INSERT INTO public.gcms_territoire (nom,code,dep,srid,geometrie) VALUES
	 ('Île Tromelin','TRO','984',32740,'POLYGON ((210000 8220000, 210000 8270000, 260000 8270000, 260000 8220000, 210000 8220000))'),
	 ('Archipel de Crozet','CRO','984',7076,'POLYGON ((400000 4800000, 400000 5000000, 700000 5000000, 700000 4800000, 400000 4800000))'),
	 ('Îles Saint-Paul et Amsterdam','SPA','984',7080,'POLYGON ((600000 5600000, 600000 5900000, 800000 5900000, 800000 5600000, 600000 5600000))'),
	 ('Métropole','FXX','FXX',2154,'POLYGON ((60000 6010000, 60000 7180000, 1270000 7180000, 1270000 6010000, 60000 6010000))');