import csv
from datetime import date

import psycopg2

from decode__diff_rnb_csv import getDiff_RNB_from_date, tri_rnb_last_changes, tri_rnb_to_remove_or_to_calculate




# connexion à la BDU Consult
# Connexion
hote_bduni = "bduni_consult.ign.fr"
port_bduni = "5432"
user_bduni = "pbm"
user_recserveur = "recserveur"
bdd_bduni = "bduni_france_consultation"
#connexion prod pbm
conn_pbm = psycopg2.connect(host=hote_bduni, database=bdd_bduni, user=user_bduni, password="")
cur_pbm = conn_pbm.cursor()
def pbm_tryexecute(sql):
    try:
        cur_pbm.execute(sql)
    except (Exception, psycopg2.Error) as error:
        print(sql)
        print(error)
    conn_pbm.commit()
def pbm_tryexecute_avc_sortie(sql):
    try:
        cur_pbm.execute(sql)
        sortie = cur_pbm.fetchall()
        conn_pbm.commit()
        print(sortie)
        return sortie
    except (Exception, psycopg2.Error) as error:
        print(sql)
        print(error)
        return ''

# récupération via l'API du fichier de diff
csv_diff_rnb = "diff_2025-06-01.csv"
# if not getDiff_RNB_from_date("2025-06-01T00:00:00Z", csv_diff_rnb):
#     print("Erreur de récupération du diff. Le programme va s'arrêter")
#     exit()

batiments_rnb_last_changes = tri_rnb_last_changes(csv_diff_rnb)


# test connexion
# pbm_tryexecute_avc_sortie('SELECT * from pbm.rnb_test_import_csv_light')

#préparation des tables temporaires pour le bati entier rnb
sql = 'DROP TABLE IF EXISTS pbm.rnb_to_calculate cascade;'
pbm_tryexecute_avc_sortie(sql)

# creation de la table receptionnant le contenu du csv last_changes
sql = '''\
    CREATE TABLE IF NOT EXISTS pbm.rnb_to_calculate (action varchar NULL,\
	rnb_id varchar NULL,\
	status varchar NULL,\
	sys_period varchar NULL,\
	point public.geometry NULL,\
	shape public.geometry NULL,\
	addresses_id varchar NULL,\
	ext_ids varchar NULL);\
	CREATE UNIQUE INDEX "pbm_rnb_to_calculate_rnb_id_pkey" ON pbm.rnb_to_calculate USING btree (rnb_id);\
    CREATE INDEX IF NOT EXISTS "pbm_rnb_to_calculate_POINT_idx" ON pbm.rnb_to_calculate USING gist (point);\
    GRANT SELECT ON pbm.rnb_to_calculate TO invite;\
    CREATE INDEX pbm_rnb_to_calculate_shape_idx ON pbm.rnb_to_calculate USING gist (shape);\
    COMMENT ON TABLE pbm.rnb_to_calculate IS 'Ne pas supprimer - 2025-07-30';\
    '''
pbm_tryexecute(sql)

sql = 'DROP TABLE IF EXISTS pbm.rnb_to_remove cascade;'
pbm_tryexecute_avc_sortie(sql)

# creation de la table receptionnant le contenu de la liste d'identifiants RNB "to_remove"
sql = '''\
    CREATE TABLE IF NOT EXISTS pbm.rnb_to_remove (rnb_id varchar NULL);\
	CREATE UNIQUE INDEX "pbm_rnb_to_remove_rnb_id_pkey" ON pbm.rnb_to_remove USING btree (rnb_id);\
    GRANT SELECT ON pbm.rnb_to_remove TO invite;\
    COMMENT ON TABLE pbm.rnb_to_remove IS 'Ne pas supprimer - 2025-07-30';\
    '''
pbm_tryexecute(sql)


# calcul des id_rnb à supprimer de la base BDTopo, ou à mettre à jour
batiments_rnb_last_changes_filtred, rnb_to_remove, rnb_to_calculate = tri_rnb_to_remove_or_to_calculate(batiments_rnb_last_changes)

# remplissage de la table rnb_to_calculate à partir du CSV last_changes en supprimant les batiments RNB qui font partie de la liste to_remove
sql_insert=''
for line_batiment_rnb in batiments_rnb_last_changes_filtred:
    sql_insert+=("INSERT INTO pbm.rnb_to_calculate (action,rnb_id,status,sys_period,point,shape,addresses_id,ext_ids) \
                 VALUES ('{0}','{1}','{2}','{3}',ST_GeomFromEWKT('{4}'),ST_GeomFromEWKT('{5}'),'{6}','{7}');").format(\
                                                           line_batiment_rnb['action'], \
                                                                 line_batiment_rnb['rnb_id'],\
                                                                 line_batiment_rnb['status'],\
                                                                 line_batiment_rnb['sys_period'],\
                                                                 line_batiment_rnb['point'],\
                                                                 line_batiment_rnb['shape'],\
                                                                 line_batiment_rnb['addresses_id'],\
                                                                 line_batiment_rnb['ext_ids'])

    # print(line_batiment_rnb)

pbm_tryexecute(sql_insert)


# remplissage de la table rnb_to_remove qui contiendra la liste des rnb_id à supprimer de la base
sql_insert=''
for rnb_id in rnb_to_remove:
    sql_insert+=("INSERT INTO pbm.rnb_to_remove (rnb_id) VALUES ('{0}');").format(rnb_id)

pbm_tryexecute(sql_insert)

# préparation de la table de réconciliation pour mettre à jour la table batiment_rnb_lien_bdtopo
sql_reconciliation_remove = '''\
    DROP TABLE IF EXISTS processus_divers.update_batiment_rnb_lien_bdtopo__batiments_rnb_destruction CASCADE;\
    CREATE TABLE processus_divers.update_batiment_rnb_lien_bdtopo__batiments_rnb_destruction AS \
    select \
        brlb.cleabs,\
        null as gcms_fingerprint, \
        true as gcms_detruit \
        from rnb_to_remove rtr \ 
    join public.batiment_rnb_lien_bdtopo brlb on identifiant_rnb = rnb_id;\
'''

pbm_tryexecute(sql_reconciliation_remove)