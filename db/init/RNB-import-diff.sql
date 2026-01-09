
-- fonction pour générer la table DELETE recserveur pour détruire les objets batiment_rnb correspondant à la table rnb_to_remove
DROP FUNCTION IF EXISTS processus_divers.rnb_to_remove_to_delete();

CREATE OR REPLACE FUNCTION processus_divers.rnb_to_remove_to_delete()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE

BEGIN

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation CASCADE;
CREATE TABLE processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_deactivation AS 
select
	brlb.cleabs,
	null as gcms_fingerprint
	from processus_divers.rnb_to_remove rtr 
join public.batiment_rnb_lien_bdtopo brlb on identifiant_rnb = rnb_id;

$$;

RETURN 'Table delete_batiment_rnb_lien_bdtopo__rnb_deactivation créée';
END;

$function$
;


-- fonction pour générer la table UPDATE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo
--------- version pour les bâtiments qui ont changé de géometrie
DROP FUNCTION IF EXISTS processus_divers.rnb_last_changes_to_update();

CREATE OR REPLACE FUNCTION processus_divers.rnb_last_changes_to_update()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE

BEGIN

EXECUTE $$ 
--------- création de la table UPDATE recserveur pour MAJ les objets batiment_rnb_lien_bdtopo
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage CASCADE;
CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__moissonnage AS 
select
	brlb.cleabs,
	null as gcms_fingerprint,
	rtc.rnb_id as identifiant_rnb,
	ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.point, 4326),gt.srid), 0),0.1)::geometry as geometrie,
	ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon) as geometrie_enveloppe,
	'{"ext_ids": ' || rtc.ext_ids || ', ' 
		|| '"created_at": "'|| rtc.created_at || '", ' 
		|| '"updated_at": "'|| rtc.updated_at || '", '
		|| '"identifiants_ban": '|| rtc.addresses_id ||'}' as informations_rnb,
	CASE 
		WHEN not ST_DWithin(ST_Multi(ST_ReducePrecision(ST_SetSRID(ST_Transform(ST_SetSRID(rtc.shape, 4326), gt.srid), 0),0.1))::geometry(Multipolygon), brlb.geometrie_enveloppe,1)
			THEN 'to_link' 
		ELSE ''
	END AS commentaire_centralise

from pbm.rnb_to_calculate rtc 
join public.gcms_territoire gt 
  	ON ST_Intersects(rtc.point, ST_Transform(ST_SetSRID(gt.geometrie, gt.srid), 4326))
join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where (rtc.action like 'update' or (rtc.action like 'create' and rtc.event_type like 'reactivation'));

$$;

RETURN 'Table update_batiment_rnb_lien_bdtopo__moissonnag créée';
END;

$function$
;

-- fonction pour générer la table DELETE recserveur pour supprimer les objets batiment_rnb_lien_bdtopo
-- pour les bâtiments qui ont le status DEMOLISHED --
DROP FUNCTION IF EXISTS processus_divers.rnb_last_changes_to_delete();

CREATE OR REPLACE FUNCTION processus_divers.rnb_last_changes_to_delete()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE

BEGIN

EXECUTE $$ 

DROP TABLE IF EXISTS processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_demolished CASCADE;
CREATE TABLE processus_divers.delete_batiment_rnb_lien_bdtopo__rnb_demolished AS 
select
	brlb.cleabs,
	null as gcms_fingerprint
	
from pbm.rnb_to_calculate rtc 

join public.batiment_rnb_lien_bdtopo brlb
	on brlb.identifiant_rnb = rtc.rnb_id
where rtc.action like 'update' and rtc.status like 'demolished';

$$;

RETURN 'Table delete_batiment_rnb_lien_bdtopo__rnb_demolished créée';
END;

$function$
;


-- fonction pour générer la table INSERT recserveur pour ajouter les objets de la table 
-- rnb_last_changes qui ne sont pas déjà dans batiment_rnb_lien_bdtopo
DROP FUNCTION IF EXISTS processus_divers.rnb_last_changes_to_insert();

CREATE OR REPLACE FUNCTION processus_divers.rnb_last_changes_to_insert()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE

BEGIN

EXECUTE $$ 

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

$$;

RETURN 'Table insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage créée';
END;

$function$
;