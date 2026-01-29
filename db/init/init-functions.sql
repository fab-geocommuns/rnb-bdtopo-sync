CREATE SCHEMA processus_divers;

DROP FUNCTION IF EXISTS processus_divers.rnb_batiment_balcon_et_maj_creation();
CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_balcon_et_maj_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des balcons   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
  

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT t.cleabs, '/') AS liens_vers_batiment,
        identifiant_rnb,
        'Batiments BDTOPO balcon' AS traitement
FROM
        (
        SELECT
                c.identifiant_rnb,
                b.cleabs
        FROM
                (
                SELECT
                        cleabs,
                        identifiant_rnb,
                        geometrie,
                        geometrie_enveloppe,
                        informations_rnb
                FROM
                        processus_divers.rnb_batiments_rnb_restant_creation a
        UNION
                SELECT
                        cleabs,
                        identifiant_rnb,
                        geometrie,
                        geometrie_enveloppe,
                        informations_rnb
                FROM
                        processus_divers.rnb_batiments_rnb_traites_creation b) c,
                processus_divers.rnb_batiments_bduni_restant_creation b
        WHERE
                @(floor(st_area(b.geometrie))::integer) < 25
                AND floor(st_area(b.geometrie))::integer < floor(st_area(c.geometrie_enveloppe))::integer
                AND st_intersects(c.geometrie_enveloppe ,
                b.geometrie)
                AND @(floor(st_area(st_intersection(c.geometrie_enveloppe, b.geometrie)))::integer) < 3) t
GROUP BY
        identifiant_rnb);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_identifiant_rnb_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_liens_vers_batiment_idx ON
processus_divers.rnb_croisements_creation(liens_vers_batiment);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;
--deplacement des bâtiments BDUni traitees dans la table rnb_batiments_bduni_traites_creation
EXECUTE $$         
INSERT
        INTO
        processus_divers.rnb_batiments_bduni_traites_creation (cleabs,
        traitement,
        identifiant_rnb,
        geometrie)
SELECT
        c.cleabs,
        c.traitement, 
        c.identifiant_rnb,
        b.geometrie
FROM 
        (
        SELECT
                DISTINCT UNNEST(string_to_array(a.liens_vers_batiment, '/')) AS cleabs,
                a.traitement,
                a.identifiant_rnb
        FROM
                processus_divers.rnb_croisements_creation a) c
LEFT JOIN processus_divers.rnb_batiments_bduni_restant_creation b
                USING (cleabs)
$$;
--deplacement des bâtiments BDUni restants dans la table rnb_batiments_bduni_restant__creation
EXECUTE $$ 
DELETE
FROM
        processus_divers.rnb_batiments_bduni_restant_creation
WHERE
        cleabs IN (
        SELECT
                cleabs
        FROM
                processus_divers.rnb_croisements_creation)
        AND cleabs != '';

$$;
--deplacement des bâtiments RNB traitees dans la table rnb_batiments_rnb_traites_creation
EXECUTE $$ 
INSERT
        INTO
        processus_divers.rnb_batiments_rnb_traites_creation(cleabs,
        identifiant_rnb,
        geometrie,
        geometrie_enveloppe,
        informations_rnb,
        traitement,
        liens_vers_batiment)
SELECT
        DISTINCT 
        b.cleabs,
        a.identifiant_rnb ,
        b.geometrie::geometry(point) ,
        b.geometrie_enveloppe::geometry(multipolygon) ,
        b.informations_rnb,
        a.traitement,
        a.liens_vers_batiment
FROM
        processus_divers.rnb_croisements_creation a
LEFT JOIN (
        SELECT
                        cleabs,
                        identifiant_rnb,
                        geometrie,
                        geometrie_enveloppe,
                        informations_rnb
        FROM
                        processus_divers.rnb_batiments_rnb_restant_creation a
UNION
        SELECT
                        cleabs,
                        identifiant_rnb,
                        geometrie,
                        geometrie_enveloppe,
                        informations_rnb
        FROM
                        processus_divers.rnb_batiments_rnb_traites_creation b) b
                USING (identifiant_rnb)
$$;

RETURN 'Traitement terminé';
END;

$function$
;

DROP FUNCTION IF EXISTS processus_divers.rnb_batiment_egaux_creation();

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_egaux_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments egaux   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
-- ajout des liaisons entre les cleabs BDuni et les identifiant_rnb dans la table de croisement 
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT b.cleabs, '/') as liens_vers_batiment,
        identifiant_rnb,
        'Bâtiments égaux' AS traitement
FROM
        processus_divers.rnb_batiments_rnb_restant_creation r,
        processus_divers.rnb_batiments_bduni_restant_creation b
WHERE
        st_equals(b.geometrie,
        r.geometrie_enveloppe)
        group by identifiant_rnb 
);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_id_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_cleabs_idx ON
processus_divers.rnb_croisements_creation(liens_vers_batiment);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_fusionnee_semblable_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments identiques   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_fusionnees CASCADE;

CREATE TABLE processus_divers.rnb_fusionnees AS 
SELECT
        DISTINCT a.geometrie_enveloppe ,
        string_agg(b.identifiant_rnb, '/') AS identifiants_rnb
FROM
        (
        SELECT
                (ST_Dump(ST_Union(c.geometrie_enveloppe))).geom AS geometrie_enveloppe
        FROM
                processus_divers.rnb_batiments_rnb_restant_creation c) a, 
                processus_divers.rnb_batiments_rnb_restant_creation b
WHERE
        st_intersects(a.geometrie_enveloppe,
        b.geometrie_enveloppe)
GROUP BY
        a.geometrie_enveloppe ;

CREATE INDEX processus_divers_rnb_fusionnees_geometry_idx ON
processus_divers.rnb_fusionnees
        USING gist (geometrie_enveloppe);

$$;

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        DISTINCT 'Batiments RNB dissous semblables' AS traitement,
        string_agg(DISTINCT cleabs, '/') AS liens_vers_batiment,
        UNNEST(string_to_array(rf.identifiants_rnb, '/')) AS identifiant_rnb
FROM
        processus_divers.rnb_batiments_bduni_restant_creation b,
        processus_divers.rnb_fusionnees rf
WHERE
        st_intersects(rf.geometrie_enveloppe ,
        b.geometrie)
                AND @(floor(ST_Area(ST_Intersection(rf.geometrie_enveloppe, b.geometrie)))) > 3
                        AND @(floor(st_area(b.geometrie))::integer - floor(st_area(rf.geometrie_enveloppe))::integer) < 3
                                AND @(floor(st_perimeter(b.geometrie))::int - floor(st_perimeter(rf.geometrie_enveloppe))::Int) < 3
                        GROUP BY
                                identifiant_rnb);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_id_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_fusionnees CASCADE;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_identique_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments identiques   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT t.cleabs, '/') as liens_vers_batiment,
        identifiant_rnb,
        'Batiments identiques' AS traitement
FROM
        (
        SELECT
                DISTINCT c.identifiant_rnb ,
                b.cleabs
        FROM
                processus_divers.rnb_batiments_rnb_restant_creation c,
                processus_divers.rnb_batiments_bduni_restant_creation b
        WHERE
                st_intersects(c.geometrie_enveloppe,
                b.geometrie)
                AND floor(st_area(st_intersection(c.geometrie_enveloppe , b.geometrie)))::integer = floor(st_area(b.geometrie))::integer
                AND floor(st_area(st_intersection(c.geometrie_enveloppe, b.geometrie)))::integer = floor(st_area(c.geometrie_enveloppe))::integer ) t 
        group by identifiant_rnb);

   
        
        
CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_identifiant_rnb_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);


GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_inclus_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments inclus   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
  

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT t.cleabs, '/') AS liens_vers_batiment,
                identifiant_rnb ,
        'Batiments inclus' AS traitement
FROM
        (
        SELECT
                DISTINCT c.identifiant_rnb ,
                b.cleabs
        FROM
                processus_divers.rnb_batiments_rnb_restant_creation c,
                processus_divers.rnb_batiments_bduni_restant_creation b
        WHERE
                ST_Contains(b.geometrie,
                ST_Buffer(c.geometrie_enveloppe ,
                -0.5))) t
GROUP BY
        identifiant_rnb );

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_id_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_non_apparie_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments qui ne seront pas appariés   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
EXECUTE $$ 
DROP TABLE IF EXISTS pg_temp.dwithin_r;

CREATE TEMP TABLE pg_temp.dwithin_r AS
SELECT
        DISTINCT UNNEST(string_to_array(r.identifiants_rnb, ', ')) AS rnb_id
FROM
        processus_divers.rnb_batiments_bduni_restant_creation b,
        (
        SELECT
                ROW_NUMBER() OVER() AS rnb_id,
                string_agg(identifiant_rnb::TEXT, ', ') AS identifiants_rnb,
                t.geometrie::geometry AS geometrie
        FROM
                (
                SELECT
                        (St_Dump(ST_Union(a.geometrie_enveloppe))).geom AS geometrie
                FROM
                        processus_divers.rnb_batiments_rnb_restant_creation AS a
) t
        JOIN processus_divers.rnb_batiments_rnb_restant_creation AS p
    ON
                ST_Intersects(p.geometrie_enveloppe ,
                t.geometrie)
        GROUP BY
                t.geometrie) r
WHERE
        ST_DWITHIN(r.geometrie,
        b.geometrie ,
        20);

CREATE UNIQUE INDEX IF NOT EXISTS pg_temp_dwithin_r_id_idx ON
pg_temp.dwithin_r(rnb_id);

$$;

EXECUTE $$ 
DROP TABLE IF EXISTS pg_temp.dwithin_b;

CREATE TEMP TABLE pg_temp.dwithin_b AS
SELECT
        DISTINCT UNNEST(string_to_array(r.cleabsg, ', ')) AS cleabs
FROM
        processus_divers.rnb_batiments_rnb_restant_creation b,
        (
        SELECT
                ROW_NUMBER() OVER() AS cleabs,
                string_agg(cleabs::TEXT, ', ') AS cleabsg,
                t.geometrie::geometry AS geometrie
        FROM
                (
                SELECT
                        (St_Dump(ST_Union(a.geometrie))).geom AS geometrie
                FROM
                        processus_divers.rnb_batiments_bduni_restant_creation AS a
) t
        JOIN processus_divers.rnb_batiments_bduni_restant_creation AS p
    ON
                ST_Intersects(p.geometrie,
                t.geometrie)
        GROUP BY
                t.geometrie) r
WHERE
        ST_DWITHIN(r.geometrie,
        b.geometrie_enveloppe ,
        20);

CREATE UNIQUE INDEX IF NOT EXISTS pg_temp_dwithin_b_id_idx ON
pg_temp.dwithin_b(cleabs);

$$;
-- ajout des liaisons entre les cleabs BDuni et les rnb_id dans la table de croisement 
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        DISTINCT b.cleabs AS liens_vers_batiment,
        '' AS identifiant_rnb ,
        'Bâtiment non apparié, à plus de 20m d''un bâtiment RNB' AS traitement
FROM
        (
        SELECT
                cleabs
        FROM
                processus_divers.rnb_batiments_bduni_restant_creation
EXCEPT
        SELECT
                cleabs
        FROM
                pg_temp.dwithin_b ) b
UNION
SELECT
        DISTINCT '' AS liens_vers_batiment,
        r.rnb_id AS identifiant_rnb ,
        'Bâtiment non apparié, à plus de 20m d''un autre bâtiment BDUni' AS traitement
FROM
        (
        SELECT
                identifiant_rnb as rnb_id
        FROM
                processus_divers.rnb_batiments_rnb_restant_creation
EXCEPT
        SELECT
                rnb_id
        FROM
                pg_temp.dwithin_r ) r);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_id_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_semblable_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments semblables   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
  

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT t.cleabs, '/') as liens_vers_batiment,
        identifiant_rnb,
        'Batiments semblables' AS traitement
FROM
        (
        SELECT
                DISTINCT c.identifiant_rnb ,
                b.cleabs
        FROM
                processus_divers.rnb_batiments_rnb_restant_creation c,
                processus_divers.rnb_batiments_bduni_restant_creation b
        WHERE
                st_intersects(c.geometrie_enveloppe ,
                b.geometrie)
                AND @(floor(ST_Area(ST_Intersection(c.geometrie_enveloppe, b.geometrie)))) > 3
                AND @(floor(st_area(b.geometrie))::integer - floor(st_area(c.geometrie_enveloppe))::integer) < 3
                AND @(floor(st_perimeter(b.geometrie))::int - floor(st_perimeter(c.geometrie_enveloppe))::Int) < 3
                AND ST_NPoints(b.geometrie) = ST_NPoints(c.geometrie_enveloppe)) t 
          group by identifiant_rnb );

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_id_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_batiment_semblable_nbp_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments semblables nombre de point différent  ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
  

EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT t.cleabs, '/') AS liens_vers_batiment,
        identifiant_rnb,
        'Batiments semblables nbpt diff' AS traitement
FROM
        (
        SELECT
                DISTINCT c.identifiant_rnb ,
                b.cleabs
        FROM
                processus_divers.rnb_batiments_rnb_restant_creation c,
                processus_divers.rnb_batiments_bduni_restant_creation b
        WHERE
                st_intersects(c.geometrie_enveloppe,
                b.geometrie)
                AND @(floor(ST_Area(ST_Intersection(c.geometrie_enveloppe, b.geometrie)))) > 3
                AND @(floor(st_area(b.geometrie))::integer - floor(st_area(c.geometrie_enveloppe))::integer) < 3
                AND @(floor(st_perimeter(b.geometrie))::int - floor(st_perimeter(c.geometrie_enveloppe))::Int) < 3
                AND ST_NPoints(b.geometrie) != ST_NPoints(c.geometrie_enveloppe)) t
GROUP BY
        identifiant_rnb );

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_identifiant_rnb_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_croisement_semantique_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------   Croisement sémantique   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
-- ajout des liaisons entre les cleabs BDuni et les rnb_id dans la table de croisement 
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
SELECT
        string_agg(DISTINCT elem->>'id', '/') AS liens_vers_batiment ,
        b.identifiant_rnb,
        'Croisement sémantique' AS traitement
FROM
        processus_divers.rnb_batiments_rnb_restant_creation b,
        jsonb_array_elements((informations_rnb::jsonb->>'ext_ids')::jsonb) AS elem,
        processus_divers.rnb_batiments_bduni_restant_creation c
WHERE
        elem->>'id' IN (
        SELECT
                cleabs
        FROM
                processus_divers.rnb_batiments_bduni_restant_creation )
GROUP BY
        (b.informations_rnb::jsonb->>'ext_ids')::jsonb,
        b.identifiant_rnb ;

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_identifiant_rnb_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_maj_des_tables_de_travail_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------   Mise à jour des tables de travail   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
---On bascule les données rnb traités dans la table      processus_divers.rnb_batiments_rnb_traites_creation et on les supprimes de la table   processus_divers.rnb_batiments_rnb_restant_creation 
----Idem pour processus_divers.rnb_batiments_bduni_traites_creation et processus_divers.rnb_batiments_bduni_restant_creation 
EXECUTE $$         
INSERT
        INTO
        processus_divers.rnb_batiments_bduni_traites_creation (cleabs,
        traitement,
        identifiant_rnb,
        geometrie)
SELECT
        c.cleabs,
        c.traitement, 
        c.identifiant_rnb,
        b.geometrie
FROM 
        (
        SELECT
                DISTINCT UNNEST(string_to_array(a.liens_vers_batiment, '/')) AS cleabs,
                a.traitement,
                a.identifiant_rnb
        FROM
                processus_divers.rnb_croisements_creation a) c
LEFT JOIN processus_divers.rnb_batiments_bduni_restant_creation b
                USING (cleabs)
WHERE
        b.geometrie IS NOT NULL           
$$;
--deplacement des bâtiments BDUni restants dans la table rnb_batiments_bduni_restant_creation
EXECUTE $$ 
DELETE
FROM
        processus_divers.rnb_batiments_bduni_restant_creation
WHERE
        cleabs IN (
        SELECT
                cleabs
        FROM
                processus_divers.rnb_batiments_bduni_traites_creation)
        AND cleabs != '';

$$;
--deplacement des bâtiments RNB traitees dans la table rnb_batiments_rnb_traites_creation
EXECUTE $$ 
INSERT
        INTO
        processus_divers.rnb_batiments_rnb_traites_creation(cleabs,
        identifiant_rnb,
        geometrie,
        geometrie_enveloppe,
        informations_rnb,
        traitement,
        liens_vers_batiment)
SELECT
        DISTINCT 
        b.cleabs,
        a.identifiant_rnb ,
        b.geometrie::geometry(point) ,
        b.geometrie_enveloppe::geometry(multipolygon) ,
        b.informations_rnb,
        a.traitement,
        a.liens_vers_batiment
FROM
        processus_divers.rnb_croisements_creation a
LEFT JOIN processus_divers.rnb_batiments_rnb_restant_creation b
                USING (identifiant_rnb)
WHERE
        b.geometrie IS NOT NULL;

$$;
--deplacement des bâtiments RNB restants dans la table rnb_batiments_rnb_restant_creation
EXECUTE $$ 
DELETE
FROM
        processus_divers.rnb_batiments_rnb_restant_creation
WHERE
        identifiant_rnb IN (
        SELECT
                identifiant_rnb
        FROM
                processus_divers.rnb_croisements_creation)
        AND identifiant_rnb != '';

$$;

RETURN 'Traitement terminé';
END;

$function$
;



CREATE OR REPLACE FUNCTION processus_divers.rnb_ponctuel_reste()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--Historique modification 

BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------  Detection des bâtiments ponctuels  ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
-- ajout des liaisons entre les cleabs BDuni et les identifiant_rnb dans la table de croisement 
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_croisements_creation CASCADE ;

CREATE TABLE processus_divers.rnb_croisements_creation AS
(
SELECT
        string_agg(DISTINCT b.cleabs, '/') as liens_vers_batiment,
        identifiant_rnb,
        'Bâtiments ponctuels' AS traitement
FROM
        processus_divers.rnb_batiments_rnb_restant_creation r,
        processus_divers.rnb_batiments_bduni_restant_creation b
WHERE
        st_intersects(b.geometrie,
        r.geometrie)
        group by identifiant_rnb 
);

CREATE INDEX IF NOT EXISTS processus_divers_batiment_rnb_croisements_creation_rnb_id_idx ON
processus_divers.rnb_croisements_creation(identifiant_rnb);


GRANT ALL ON
processus_divers.rnb_croisements_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_croisements_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;

CREATE OR REPLACE FUNCTION processus_divers.rnb_preparation_creation()
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
--Phases
--create table processus_divers.stat_dep qui permet de donner des indicateurs sur le nb de bâtiments RNB-BDUni liés à chaque étape
--Historique modification 


BEGIN
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------   Préparation des tables de travail   ---------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
--creation de la table statistique qui permet de donner des indicateurs sur le nb de bâtiments RNB mis à jour ------------------------------------------  
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.stat_rnb CASCADE ;

CREATE TABLE processus_divers.stat_rnb AS
SELECT
        0 AS nb_de_insert,
        0 AS nb_de_delete,
        0 AS nb_de_update,
        0 AS nb_batiment_bdu_apparies
LIMIT 0;

GRANT ALL ON
processus_divers.stat_rnb TO pbm;

GRANT ALL ON
processus_divers.stat_rnb TO recserveur;

$$;
--creation de la table RNB polygons pas encore traités ------------------------------------------  
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_restant_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_rnb_restant_creation AS
SELECT
        identifiant_rnb,
        geometrie,
        geometrie_enveloppe,
        informations_rnb::varchar
FROM
        recserveur.insert_batiment_rnb_lien_bdtopo__creationerentiel
UNION 
SELECT
        identifiant_rnb,
        geometrie,
        geometrie_enveloppe,
        informations_rnb::varchar
FROM
        recserveur.update_batiment_rnb_lien_bdtopo__creationerentiel;

CREATE INDEX processus_divers_rnb_batiments_rnb_restant_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_restant_creation
        USING gist (geometrie);

CREATE INDEX processus_divers_rnb_batiments_rnb_restant_creation_geometrie_env_idx ON
processus_divers.rnb_batiments_rnb_restant_creation
        USING gist (geometrie_enveloppe);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_rnb_restant_creation_identifiant_rnb_idx ON
processus_divers.rnb_batiments_rnb_restant_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_batiments_rnb_restant_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_batiments_rnb_restant_creation TO recserveur;

$$;
--creation de la table RNB polygons déjà traités ------------------------------------------  
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_traites_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_rnb_traites_creation AS
        SELECT
        *,
        ''::TEXT AS traitement,
        '' AS liens_vers_batiment
FROM
        processus_divers.rnb_batiments_rnb_restant_creation
LIMIT 0;

CREATE INDEX processus_divers_rnb_batiments_rnb_traites_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_traites_creation
        USING gist (geometrie);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_rnb_traites_creation_identifiant_rnb_idx ON
processus_divers.rnb_batiments_rnb_traites_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_batiments_rnb_traites_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_batiments_rnb_traites_creation TO recserveur;

$$;
--creation de la table BDUni polygons pas encore traités ------------------------------------------  
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_restant_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_bduni_restant_creation AS
        SELECT
        DISTINCT a.cleabs,
        st_force2d(a.geometrie) AS geometrie
FROM
        public.batiment a,
        processus_divers.rnb_batiments_rnb_restant_creation b
WHERE
        NOT a.gcms_detruit
        AND ST_INTERSECTS(a.geometrie,
        st_buffer(b.geometrie,
        500)) ;

CREATE INDEX processus_divers_rnb_batiments_bduni_restant_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_restant_creation
        USING gist (geometrie);

CREATE UNIQUE INDEX IF NOT EXISTS processus_divers_rnb_batiments_bduni_restant_creation_ID_idx ON
processus_divers.rnb_batiments_bduni_restant_creation(cleabs);

GRANT ALL ON
processus_divers.rnb_batiments_bduni_restant_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_batiments_bduni_restant_creation TO recserveur;

$$;
--creation de la table BDUni polygons déjà traités ------------------------------------------  
EXECUTE $$ 
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_traites_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_bduni_traites_creation AS
        SELECT
        cleabs,
        ''::TEXT AS traitement,
        ''::TEXT AS identifiant_rnb ,
        st_force2d(geometrie) AS geometrie
FROM
        processus_divers.batiment
LIMIT 0;

CREATE INDEX processus_divers_rnb_batiments_bduni_traites_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_traites_creation
        USING gist (geometrie);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_bduni_traites_creation_ID_idx ON
processus_divers.rnb_batiments_bduni_traites_creation(cleabs);

GRANT ALL ON
processus_divers.rnb_batiments_bduni_traites_creation TO pbm;

GRANT ALL ON
processus_divers.rnb_batiments_bduni_traites_creation TO recserveur;

$$;

RETURN 'Traitement terminé';
END;

$function$
;


CREATE OR REPLACE FUNCTION processus_divers.rnb_maj_liens_vers_batiment_car_maj_batiment_rnb()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
--DECLARE
BEGIN
--IF EXTRACT(DOW
--FROM
--NOW() ) <> 2 THEN RETURN 0;
--END IF;


-- On récupère les bâtiments RNB qui ont été créés ou modifiés depuis la veille (cf. integration du différentiel RNB) 
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_restant_creation;

CREATE TABLE processus_divers.rnb_batiments_rnb_restant_creation AS 
SELECT
        DISTINCT ON
        (rnb.cleabs)
        rnb.cleabs,
        rnb.identifiant_rnb,
        rnb.geometrie,
        rnb.geometrie_enveloppe,
        rnb.informations_rnb::varchar
FROM
        pbm.batiment_rnb_lien_bdtopo rnb
WHERE
        NOT gcms_detruit
        AND (gcms_date_creation >= CURRENT_DATE - INTERVAL '2 days'
				OR gcms_date_modification >= CURRENT_DATE - INTERVAL '2 days');

CREATE INDEX processus_divers_rnb_batiments_rnb_restant_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_restant_creation
        USING gist (geometrie);

CREATE INDEX processus_divers_rnb_batiments_rnb_restant_creation_geometrie_env_idx ON
processus_divers.rnb_batiments_rnb_restant_creation
        USING gist (geometrie_enveloppe);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_rnb_restant_creation_identifiant_rnb_idx ON
processus_divers.rnb_batiments_rnb_restant_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_batiments_rnb_restant_creation TO pbm;

-- On fait un buffer de 100m sur les geometries rnb de creation/modification et on les fusionne
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_buffer_creation;

CREATE TABLE processus_divers.rnb_batiments_rnb_buffer_creation AS 
SELECT
        (ST_Dump(st_union(st_buffer(geometrie,100)))).geom AS geometrie
FROM
        processus_divers.rnb_batiments_rnb_restant_creation ;

CREATE INDEX processus_divers_rnb_batiments_rnb_buffer_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_buffer_creation
        USING gist (geometrie);

-- On récupère les bâtiments BDUni qui sont proches des bâtiments RNB à apparier
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_restant_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_bduni_restant_creation AS
SELECT
        DISTINCT bdu.cleabs,
        st_force2d(bdu.geometrie) AS geometrie
FROM
        public.batiment bdu,
        processus_divers.rnb_batiments_rnb_buffer_creation rnb
WHERE

        st_intersects(bdu.geometrie,
        rnb.geometrie)
        AND NOT bdu.gcms_detruit ;

CREATE INDEX processus_divers_rnb_batiments_bduni_restant_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_restant_creation
        USING gist (geometrie);

CREATE UNIQUE INDEX IF NOT EXISTS processus_divers_rnb_batiments_bduni_restant_creation_ID_idx ON
processus_divers.rnb_batiments_bduni_restant_creation(cleabs);

GRANT ALL ON
processus_divers.rnb_batiments_bduni_restant_creation TO pbm;

--creation de la table RNB polygons déjà traités ------------------------------------------  
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_traites_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_rnb_traites_creation AS
        SELECT
        *,
        ''::TEXT AS traitement,
        '' AS liens_vers_batiment
FROM
        processus_divers.rnb_batiments_rnb_restant_creation
LIMIT 0;

CREATE INDEX processus_divers_rnb_batiments_rnb_traites_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_traites_creation
        USING gist (geometrie);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_rnb_traites_creation_identifiant_rnb_idx ON
processus_divers.rnb_batiments_rnb_traites_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_batiments_rnb_traites_creation TO pbm;

--creation de la table BDUni polygons déjà traités ------------------------------------------  
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_traites_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_bduni_traites_creation AS
        SELECT
        cleabs,
        ''::TEXT AS traitement,
        ''::TEXT AS identifiant_rnb ,
        st_force2d(geometrie) AS geometrie
FROM
        public.batiment
LIMIT 0;

CREATE INDEX processus_divers_rnb_batiments_bduni_traites_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_traites_creation
        USING gist (geometrie);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_bduni_traites_creation_ID_idx ON
processus_divers.rnb_batiments_bduni_traites_creation(cleabs);

GRANT ALL ON
processus_divers.rnb_batiments_bduni_traites_creation TO pbm;


--batiment non appariables
PERFORM
        *
FROM
        processus_divers.rnb_batiment_non_apparie_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;

--traitement sémantique
PERFORM
        processus_divers.rnb_croisement_semantique_creation(); 
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;

--traitement bâtiments semblables
PERFORM
        processus_divers.rnb_batiment_semblable_creation();         
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;


--traitement bâtiments semblables nb pt différents
PERFORM
        processus_divers.rnb_batiment_semblable_nbp_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;


--traitement bâtiments fusion semblables 
PERFORM
        processus_divers.rnb_batiment_fusionnee_semblable_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;



--traitement bâtiments inclus
PERFORM
        processus_divers.rnb_batiment_inclus_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;


--traitement bâtiments balcon
PERFORM
        processus_divers.rnb_batiment_balcon_et_maj_creation();

--select * from processus_divers.rnb_croisements_creation;

--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;




--on créé la table recserveur (dans processus divers pour faire une poire auto) pour mettre à jour les liens_vers_batiment de la table batiment_rnb_lien_bdtopo
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__batiment_rnb_creation CASCADE;

CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__batiment_rnb_creation
AS
SELECT
    cleabs,
    STRING_AGG(DISTINCT liens_vers_batiment, '/') AS liens_vers_batiment,
    STRING_AGG(DISTINCT traitement, '/') AS traitement,
    NULL AS gcms_fingerprint
FROM (
    SELECT
        t1.cleabs,
        split_liens AS liens_vers_batiment,
        t1.traitement
    FROM (
        SELECT
            cleabs,
            traitement,
            regexp_split_to_table(liens_vers_batiment, '/') AS split_liens
        FROM processus_divers.rnb_batiments_rnb_traites_creation
        WHERE traitement != 'Bâtiment non apparié, à plus de 20m d''un autre bâtiment BDUni'
    ) t1

    UNION

    SELECT
        t4.cleabs,
        split_liens,
        t4.traitement
    FROM (
        SELECT
            t2.cleabs,
            t3.traitement,
            regexp_split_to_table(t2.liens_vers_batiment, '/') AS split_liens
        FROM pbm.batiment_rnb_lien_bdtopo t2
        JOIN processus_divers.rnb_batiments_rnb_traites_creation t3
            ON t2.cleabs = t3.cleabs
        WHERE t3.traitement != 'Bâtiment non apparié, à plus de 20m d''un autre bâtiment BDUni'
    ) t4
) AS t
GROUP BY cleabs;

RETURN 1;

END$function$
;

DROP FUNCTION IF EXISTS processus_divers.rnb_maj_liens_vers_batiment_car_creation_batiment_bdtopo();

CREATE FUNCTION processus_divers.rnb_maj_liens_vers_batiment_car_creation_batiment_bdtopo() RETURNS integer LANGUAGE PLPGSQL AS $$
DECLARE
BEGIN
IF EXTRACT(DOW
FROM
NOW() ) <> 5 THEN RETURN 0;
END IF;


-- On récupère les bâtiments de la bduni qui ont été créés dans les 8 derniers jours 
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_restant_creation;

CREATE TABLE processus_divers.rnb_batiments_bduni_restant_creation AS 
SELECT
        DISTINCT cleabs,
        st_force2d(geometrie) AS geometrie
FROM
        public.batiment
WHERE
        NOT gcms_detruit
        AND gcms_date_creation >= CURRENT_DATE - INTERVAL '8 days' ;

CREATE INDEX processus_divers_rnb_batiments_bduni_restant_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_restant_creation
        USING gist (geometrie);

CREATE UNIQUE INDEX IF NOT EXISTS processus_divers_rnb_batiments_bduni_restant_creation_ID_idx ON
processus_divers.rnb_batiments_bduni_restant_creation(cleabs);

GRANT ALL ON
processus_divers.rnb_batiments_bduni_restant_creation TO pbm;

-- On fait un buffer de 100m sur les geometries bduni de creation et on les fusionne
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_buffer_creation;

CREATE TABLE processus_divers.rnb_batiments_bduni_buffer_creation AS 
SELECT
        (ST_Dump(st_union(st_buffer(geometrie,100)))).geom AS geometrie
FROM
        processus_divers.rnb_batiments_bduni_restant_creation ;

CREATE INDEX processus_divers_rnb_batiments_bduni_buffer_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_buffer_creation
        USING gist (geometrie);

-- On récupère les bâtiments RNB (batiment_rnb_lien_bdtopo) qui sont proches des bâtiments BDuni créés
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_restant_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_rnb_restant_creation AS
SELECT
        DISTINCT ON
        (rnb.cleabs)
        rnb.cleabs,
        rnb.identifiant_rnb,
        rnb.geometrie,
        rnb.geometrie_enveloppe,
        rnb.informations_rnb::varchar
FROM
        public.batiment_rnb_lien_bdtopo rnb,
        processus_divers.rnb_batiments_bduni_buffer_creation bdu
WHERE
        --st_intersects(bdu.geometrie,
        --rnb.geometrie_enveloppe)
        --OR 
        st_intersects(bdu.geometrie,
        rnb.geometrie)
        AND NOT rnb.gcms_detruit ;

CREATE INDEX processus_divers_rnb_batiments_rnb_restant_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_restant_creation
        USING gist (geometrie);

CREATE INDEX processus_divers_rnb_batiments_rnb_restant_creation_geometrie_env_idx ON
processus_divers.rnb_batiments_rnb_restant_creation
        USING gist (geometrie_enveloppe);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_rnb_restant_creation_identifiant_rnb_idx ON
processus_divers.rnb_batiments_rnb_restant_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_batiments_rnb_restant_creation TO pbm;

--creation de la table RNB polygons déjà traités ------------------------------------------  
DROP TABLE IF EXISTS processus_divers.rnb_batiments_rnb_traites_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_rnb_traites_creation AS
        SELECT
        *,
        ''::TEXT AS traitement,
        '' AS liens_vers_batiment
FROM
        processus_divers.rnb_batiments_rnb_restant_creation
LIMIT 0;

CREATE INDEX processus_divers_rnb_batiments_rnb_traites_creation_geometry_idx ON
processus_divers.rnb_batiments_rnb_traites_creation
        USING gist (geometrie);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_rnb_traites_creation_identifiant_rnb_idx ON
processus_divers.rnb_batiments_rnb_traites_creation(identifiant_rnb);

GRANT ALL ON
processus_divers.rnb_batiments_rnb_traites_creation TO pbm;

--creation de la table BDUni polygons déjà traités ------------------------------------------  
DROP TABLE IF EXISTS processus_divers.rnb_batiments_bduni_traites_creation CASCADE ;

CREATE TABLE processus_divers.rnb_batiments_bduni_traites_creation AS
        SELECT
        cleabs,
        ''::TEXT AS traitement,
        ''::TEXT AS identifiant_rnb ,
        st_force2d(geometrie) AS geometrie
FROM
        public.batiment
LIMIT 0;

CREATE INDEX processus_divers_rnb_batiments_bduni_traites_creation_geometry_idx ON
processus_divers.rnb_batiments_bduni_traites_creation
        USING gist (geometrie);

CREATE INDEX IF NOT EXISTS processus_divers_rnb_batiments_bduni_traites_creation_ID_idx ON
processus_divers.rnb_batiments_bduni_traites_creation(cleabs);

GRANT ALL ON
processus_divers.rnb_batiments_bduni_traites_creation TO pbm;


--batiment non appariables
PERFORM
        *
FROM
        processus_divers.rnb_batiment_non_apparie_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;

--traitement sémantique
PERFORM
        processus_divers.rnb_croisement_semantique_creation(); 
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;

--traitement bâtiments semblables
PERFORM
        processus_divers.rnb_batiment_semblable_creation();         
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;


--traitement bâtiments semblables nb pt différents
PERFORM
        processus_divers.rnb_batiment_semblable_nbp_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;


--traitement bâtiments fusion semblables 
PERFORM
        processus_divers.rnb_batiment_fusionnee_semblable_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;



--traitement bâtiments inclus
PERFORM
        processus_divers.rnb_batiment_inclus_creation();
--select * from processus_divers.rnb_croisements_creation;
PERFORM
        processus_divers.rnb_maj_des_tables_de_travail_creation();
--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;


--traitement bâtiments balcon
PERFORM
        processus_divers.rnb_batiment_balcon_et_maj_creation();

--select * from processus_divers.rnb_croisements_creation;

--select * from processus_divers.rnb_batiments_rnb_restant_creation; 
--select * from processus_divers.rnb_batiments_bduni_restant_creation;
--select * from processus_divers.rnb_batiments_rnb_traites_creation;
--select * from processus_divers.rnb_batiments_bduni_traites_creation;




--on créé la table recserveur (dans processus divers pour faire une poire auto) pour mettre à jour les liens_vers_batiment de la table batiment_rnb_lien_bdtopo
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_creation CASCADE;

CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_creation
AS
SELECT
        cleabs,
        STRING_AGG(t.liens_vers_batiment, '/') AS liens_vers_batiment,
        null AS gcms_fingerprint
FROM
        (
        SELECT
                distinct cleabs,
                regexp_split_to_table( t1.liens_vers_batiment, '/') as liens_vers_batiment
        FROM
                processus_divers.rnb_batiments_rnb_traites_creation t1
        WHERE
                t1.traitement != 'Bâtiment non apparié, à plus de 20m d''un autre bâtiment BDUni'
        UNION   
        SELECT
                DISTINCT t2.cleabs,
                regexp_split_to_table( t2.liens_vers_batiment, '/') as liens_vers_batiment
        FROM
                public.batiment_rnb_lien_bdtopo t2,
                processus_divers.rnb_batiments_rnb_traites_creation t3
        WHERE
                t3.traitement != 'Bâtiment non apparié, à plus de 20m d''un autre bâtiment BDUni'
                AND t2.cleabs = t3.cleabs
                --AND t2.liens_vers_batiment IS NOT NULL
        ) t
GROUP BY
        t.cleabs
;

RETURN 1;

END$$;

DROP FUNCTION IF EXISTS processus_divers.rnb_maj_liens_vers_batiment_car_suppr_batiment_bdtopo();

CREATE FUNCTION processus_divers.rnb_maj_liens_vers_batiment_car_suppr_batiment_bdtopo() RETURNS integer LANGUAGE PLPGSQL AS $$
DECLARE
BEGIN
IF EXTRACT(DOW
FROM
NOW() ) <> 4 THEN RETURN 0;
END IF;
-- On récupère les bâtiments de la bduni qui ont été supprimés dans les 8 derniers jours 
DROP TABLE IF EXISTS processus_divers.batiment_suppr;

CREATE TEMP TABLE processus_divers.batiment_suppr AS 
SELECT
        DISTINCT cleabs
FROM
        public.batiment
WHERE
        gcms_detruit
        AND gcms_date_destruction >= CURRENT_DATE - INTERVAL '8 days' ;

CREATE INDEX IF NOT EXISTS batiment_suppr_cleabs_idx ON
processus_divers.batiment_suppr
        USING btree (cleabs);
--select * from processus_divers.batiment_suppr ;
-- On eclate les liens_vers_batiment (bdtopo) de la table delete_batiment_rnb_lien_bdtopo pour mieux les manipuler
DROP TABLE IF EXISTS processus_divers.lien_rnb_bdtopo CASCADE;

CREATE TABLE processus_divers.lien_rnb_bdtopo AS
SELECT
        cleabs,
        gcms_fingerprint,
        -- éclate les liens multiples (mettre un autre nom que bdtopo ?)
        regexp_split_to_table(COALESCE(liens_vers_batiment, ''), '/') AS lien_vers_batiment,
        -- permettra de marquer les liens détruits (pour le différentiel)
  FALSE AS lien_detruit
        -- le gcms_numrec de la table RNB
FROM
        public.batiment_rnb_lien_bdtopo
WHERE
        NOT gcms_detruit;

CREATE INDEX IF NOT EXISTS lien_rnb_bdtopo_lien_vers_batiment_idx ON
processus_divers.lien_rnb_bdtopo
        USING btree (lien_vers_batiment);

CREATE INDEX IF NOT EXISTS lien_rnb_bdtopo_cleabs_idx ON
processus_divers.lien_rnb_bdtopo
        USING btree (cleabs);
--select * from pg_temp.lien_rnb_bdtopo;  
-- On identifie les cleabs de la table batiment_rnb_lien_bdtopo pour lesquel une cleabs bdtopo suppr est renseignée dans liens_vers_batiment 
DROP TABLE IF EXISTS processus_divers.cleabs_rnb_a_reprendre CASCADE;

CREATE TABLE processus_divers.cleabs_rnb_a_reprendre AS
SELECT
        DISTINCT r.cleabs,
        r.lien_vers_batiment
FROM
        processus_divers.lien_rnb_bdtopo r,
        processus_divers.batiment_suppr b
WHERE
        r.lien_vers_batiment = b.cleabs ;

CREATE INDEX IF NOT EXISTS cleabs_rnb_a_reprendre_lien_vers_batiment_idx ON
processus_divers.cleabs_rnb_a_reprendre
        USING btree (lien_vers_batiment);

CREATE INDEX IF NOT EXISTS cleabs_rnb_a_reprendre_cleabs_idx ON
processus_divers.cleabs_rnb_a_reprendre
        USING btree (cleabs);
--select * from pg_temp.cleabs_rnb_a_reprendre;  
--on créé la table recserveur (dans processus divers pour faire une poire auto) pour mettre à jour les liens_vers_batiment de la table batiment_rnb_lien_bdtopo
DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr CASCADE;

CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr
AS
SELECT
        t1.cleabs,
        t1.gcms_fingerprint,
        STRING_AGG(t1.lien_vers_batiment, '/') AS liens_vers_batiment
FROM
        processus_divers.lien_rnb_bdtopo t1
WHERE
        NOT EXISTS (
        SELECT
                1
        FROM
                processus_divers.cleabs_rnb_a_reprendre t2
        WHERE
                t1.lien_vers_batiment = t2.lien_vers_batiment)
        -- On ne garde que les valeurs non présentes dans table2
        AND EXISTS (
        -- Assurer qu'au moins une valeur de table1 est présente dans table2 pour chaque id
        SELECT
                1
        FROM
                processus_divers.cleabs_rnb_a_reprendre t2
        WHERE
                t2.cleabs = t1.cleabs 
    )
GROUP BY
        t1.cleabs,
        t1.gcms_fingerprint 
;
--select * from processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr; 
--on vide le liens_vers_batiment s'il n'y avait que des bâtiments bdtopo supprimés de renseignés. 
INSERT
        INTO
        processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr(cleabs,
        gcms_fingerprint,
        liens_vers_batiment)
SELECT
        DISTINCT
    t1.cleabs,
        t1.gcms_fingerprint,
        NULL AS liens_vers_batiment
FROM
        processus_divers.lien_rnb_bdtopo t1
WHERE
        EXISTS (
        SELECT
                1
        FROM
                processus_divers.cleabs_rnb_a_reprendre t2
        WHERE
                t2.cleabs = t1.cleabs)
        AND 
    NOT EXISTS (
        SELECT
                1
        FROM
                processus_divers.update_batiment_rnb_lien_bdtopo__batiment_bdtopo_suppr t2
        WHERE
                t2.cleabs = t1.cleabs);

RETURN 1;

END$$;
