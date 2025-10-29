

--------- création de la table DELETE recserveur pour détruire les objets batiment_rnb correspondant à la table rnb_to_remove
DROP TABLE IF EXISTS processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation CASCADE;
CREATE TABLE processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation AS 
select
	brlb.cleabs,
	null as gcms_fingerprint
	from rnb_to_remove rtr 
join public.batiment_rnb_lien_bdtopo brlb on identifiant_rnb = rnb_id;


--------- création de la table UPDATE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo correspondants à la table rnb_to_calculate
--------- version pour les bâtiments qui ont changé de géometrie
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_diff CASCADE;
CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_diff AS 
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0),0.1)::geometry as geometrie,
	ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon) as geometrie_enveloppe,
	json_build_object(
		'ext_ids', rtc.ext_ids,
		'created_at', rtc.created_at,
		'updated_at', rtc.updated_at,
		'status', rtc.status,
		'event_type', rtc.event_type,
		'parent_buildings', rtc.parent_buildings,
		'identifiants_ban', rtc.addresses_id
	)::varchar as informations_rnb,
	'to_link' as commentaire_centralise

from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where (rtc.action like 'update' or (rtc.action like 'create' and rtc.event_type like 'reactivation'))
and not ST_DWithin(ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon), brlb.geometrie_enveloppe,1)
;


--------- création de la table UPDATE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo correspondants à la table rnb_to_calculate
--------- version pour les bâtiments qui ont changé de géometrie
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_diff CASCADE;
CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_diff AS 
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0),0.1)::geometry as geometrie,
	ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon) as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' 
		|| '"created_at": "'|| rtc.created_at || '", ' 
		|| '"updated_at": "'|| rtc.updated_at || '", '
		|| '"status": "'|| rtc.status || '", '
		|| '"event_type": "'|| rtc.event_type || '", '
		|| '"parent_buildings": '|| rtc.parent_buildings ||', '
		|| '"identifiants_ban": '|| rtc.addresses_id ||'}'::json as informations_rnb,
	'to_link' as commentaire_centralise

from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where (rtc.action like 'update' or (rtc.action like 'create' and rtc.event_type like 'reactivation'))
and not ST_DWithin(ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon), brlb.geometrie_enveloppe,1)
;

--------- création de la table UPDATE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo correspondants à la table rnb_to_calculate
--------- version pour les bâtiments qui N'ONT PAS changé de géometrie
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_equal CASCADE;
CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_equal AS 
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0),0.1)::geometry as geometrie,
	ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon) as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' 
		|| '"created_at": "'|| rtc.created_at || '", ' 
		|| '"updated_at": "'|| rtc.updated_at || '", '
		|| '"status": "'|| rtc.status || '", '
		|| '"event_type": "'|| rtc.event_type || '", '
		|| '"parent_buildings": '|| rtc.parent_buildings ||', '
		|| '"identifiants_ban": '|| rtc.addresses_id ||'}'::json as informations_rnb
		
from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where (rtc.action like 'update' or (rtc.action like 'create' and rtc.event_type like 'reactivation'))
and ST_DWithin(ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon), brlb.geometrie_enveloppe,1)
;

--------- création de la table UPDATE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo correspondants à la table rnb_to_calculate
--------- version TOTALE
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_total CASCADE;
CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_total AS 
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0),0.1)::geometry as geometrie,
	ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon) as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' 
		|| '"created_at": "'|| rtc.created_at || '", ' 
		|| '"updated_at": "'|| rtc.updated_at || '", '
		|| '"status": "'|| rtc.status || '", '
		|| '"event_type": "'|| rtc.event_type || '", '
		|| '"parent_buildings": '|| rtc.parent_buildings ||', '
		|| '"identifiants_ban": '|| rtc.addresses_id ||'}'::json as informations_rnb
		
from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where rtc.action like 'update' 
or (rtc.action like 'create' and rtc.event_type like 'reactivation');


--------- création de la table DELETE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo 
--------- de la table rnb_to_calculate qui ont le status DEMOLISHED --
DROP TABLE IF EXISTS processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_demolished CASCADE;
CREATE TABLE processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_demolished AS 
select
	brlb.cleabs,
	null as gcms_fingerprint
	
from pbm.rnb_to_calculate rtc 

join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where rtc.action like 'update' and rtc.status like 'demolished';



--------- création de la table INSERT recserveur pour ajouter les objets de la table 
--------- rnb_to_calculate qui ne sont pas déjà dans batiment_rnb_lien_bdtopo
DROP TABLE IF EXISTS processus_divers.insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage CASCADE;
CREATE TABLE processus_divers.insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage AS 
select

	rtc.rnb_id as identifiant_rnb,
	ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0),0.1)::geometry as geometrie,
	ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon) as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' || '"created_at": "'|| rtc.created_at || '", ' || '"updated_at": "'|| rtc.updated_at || '", ' || '"identifiants_ban": ' || rtc.addresses_id ||'}' as informations_rnb,
	rtc.action,
	rtc.status

from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
left join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where brlb.identifiant_rnb is null
and ((rtc.action like 'update' and rtc.status like 'constructed') or rtc.action like 'create');





SELECT
        processus_divers.rnb_maj_liens_vers_batiment_car_maj_batiment_rnb();



------ DEBUG ----------------------------------------
select count(*) from rnb_test_import_diff rtid 

select count(*) from rnb_to_calculate rtc 

select count(*) from processus_divers.update_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage


select count(*) from rnb_stock_import rsi

select * from rnb_to_calculate rtc where ST_AsText(shape) not like 'MULTIPOLY%'

select * from rnb_to_remove rtr 
join public.batiment_rnb_lien_bdtopo brlb on identifiant_rnb = rnb_id

select count(*) from rnb_to_remove rtr 
join public.batiment_rnb_lien_bdtopo brlb on identifiant_rnb = rnb_id

select count(*) from rnb_to_remove rtr 

with insert_rnb as(
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0)::geometry as geometrie,
	ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0)::geometry as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' || '"created_at": "'|| rtc.created_at || '", ' || '"updated_at": "'|| rtc.updated_at || '", ' || '"identifiants_ban": ' || rtc.addresses_id ||'}' as informations_rnb,
	rtc.action,
	rtc.status

from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
left join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where brlb.identifiant_rnb is null
and ((rtc.action like 'update' and rtc.status like 'constructed') or rtc.action like 'create')
--and rtc.action like 'create'

),
--select count(*) from insert_rnb

update_rnb as (
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	case rtc.status
		when 'demolished' then true
		else
			false
	end as gcms_detruit,
	ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0)::geometry as geometrie,
	ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0)::geometry as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' || '"created_at": "'|| rtc.created_at || '", ' || '"updated_at": "'|| rtc.updated_at || '", ' || '"identifiants_ban": ' || rtc.addresses_id ||'}' as informations_rnb

from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where rtc.action like 'update'
or (rtc.action like 'create' and rtc.event_type like 'reactivation')
)

select * from rnb_to_calculate rtc 
left join public.gcms_territoire gt ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
where rnb_id not in (
	select identifiant_rnb from update_rnb 
	union
	select identifiant_rnb from insert_rnb
	)


select *,
	ST_Transform(ST_SetSRID(ST_Expand(gt.geometrie,50), gt.srid), 4326) as geom_transfo
from public.gcms_territoire gt 
select *
from public.gcms_territoire gt 

-- traitement des batiments "demolished"
select * from pbm.rnb_to_calculate rtc 
join public.batiment_rnb_lien_bdtopo brlb on rtc.rnb_id = brlb.identifiant_rnb
where status like 'demolished'

select * from public.batiment_rnb_lien_bdtopo brlb where informations_rnb like '%ban%' and gcvs_nom_lot like 'RNB_dep_75'



--------- création de la table test batiment_rnb_lien_bdtopo_test pour ajouter les objets de la table 
--------- rnb_to_calculate qui ne sont pas déjà dans batiment_rnb_lien_bdtopo
DROP TABLE IF EXISTS pbm.batiment_rnb_lien_bdtopo CASCADE;

CREATE TABLE pbm.batiment_rnb_lien_bdtopo (
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
	CONSTRAINT batiment_rnb_lien_bdtopo_pkey PRIMARY KEY (cleabs),
	CONSTRAINT enforce_srid_empreinte CHECK ((st_srid(gcvs_empreinte) = 0)),
	CONSTRAINT enforce_srid_geometrie CHECK ((st_srid(geometrie) = 0)),
	CONSTRAINT liens_vers_batiment_check_lien CHECK (ign_bduni_check_lien(cleabs, gcms_detruit, 'batiment_rnb_lien_bdtopo'::character varying, 'liens_vers_batiment'::character varying, liens_vers_batiment, 'MULTILIEN'::character varying))
);
CREATE INDEX batiment_rnb_lien_bdtopo_gcms_numrec_idx ON pbm.batiment_rnb_lien_bdtopo USING btree (gcms_numrec);
CREATE INDEX batiment_rnb_lien_bdtopo_gcms_territoire_idx ON pbm.batiment_rnb_lien_bdtopo USING btree (gcms_territoire);
CREATE INDEX batiment_rnb_lien_bdtopo_gcvs_empreinte_idx ON pbm.batiment_rnb_lien_bdtopo USING gist (gcvs_empreinte);
CREATE INDEX batiment_rnb_lien_bdtopo_geometrie_idx ON pbm.batiment_rnb_lien_bdtopo USING gist (geometrie);
CREATE UNIQUE INDEX batiment_rnb_lien_bdtopo_identifiant_rnb_gcms_numrec_idx ON pbm.batiment_rnb_lien_bdtopo USING btree (identifiant_rnb, gcms_numrec);
CREATE UNIQUE INDEX batiment_rnb_lien_bdtopo_identifiant_rnb_idx ON pbm.batiment_rnb_lien_bdtopo USING btree (identifiant_rnb);
CREATE INDEX batiment_rnb_lien_bdtopo_liens_vers_batiment_idx ON pbm.batiment_rnb_lien_bdtopo USING btree (liens_vers_batiment);
insert into pbm.batiment_rnb_lien_bdtopo 

select * 

	 from public.batiment_rnb_lien_bdtopo brlb
	

limit 10;


insert into pbm.batiment_rnb_lien_bdtopo 

select * 

	 from public.batiment_rnb_lien_bdtopo brlb
	 where cleabs = 'BAT_RNB_0000002435060184'
	


update pbm.batiment_rnb_lien_bdtopo 
set gcms_date_creation = now()



--- export de batiments_rnb_lien_bdtopo correspondant à update_batiment_rnb_lien_bdtopo__moissonnage_geom_diff
select rnb.identifiant_rnb,rnb.geometrie_enveloppe  from public.batiment_rnb_lien_bdtopo rnb
join processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage_geom_diff uprnb on rnb.identifiant_rnb = uprnb.identifiant_rnb



--- ajout d'un champ parent_buildings dans la table rnb_to_calculate (valeur par défaut en attendant que le Python recrée la table)
alter table pbm.rnb_to_calculate add parent_buildings varchar
update pbm.rnb_to_calculate set parent_buildings = '["4MM5ZAAKHFCQ", "S8JQZTJCZC3X"]'
["4MM5ZAAKHFCQ", "S8JQZTJCZC3X"]