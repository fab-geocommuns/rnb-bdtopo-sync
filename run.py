import datetime
from datetime import datetime, timedelta
from db import (
    get_connection, 
    get_connection_recserveur, 
    get_cursor, 
    create_last_changes_table, 
    create_to_remove_table, 
    is_table_empty
)
from rnb import (
    getDiff_RNB_from_date,
    getDiff_RNB_from_file,
    convert_rnb_diff,
    persist_last_changes,
    persist_to_remove,
    prepare_recserveur_tables_for_last_changes,
    prepare_recserveur_tables_for_to_remove
)
from recserveur import launch_poires


def sync_rnb(since: datetime) -> tuple[list, set]:
    # Télécharger le fichier de diff
    rnb_diff = getDiff_RNB_from_date(since)

    _from_diff_to_db(rnb_diff)
    # _merge_rnb_diff()
     

def sync_rnb_from_file(filename: str) -> tuple[list, set]:
    rnb_diff = getDiff_RNB_from_file(filename)

    _from_diff_to_db(rnb_diff)
    # _merge_rnb_diff()
     


def _from_diff_to_db(diff):

    last_changes, to_remove = convert_rnb_diff(diff)

    # Insert last_changes and to_remove in the database
    today = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        with get_cursor(conn) as cursor:
            create_to_remove_table(cursor, today)
            create_last_changes_table(cursor, today)
            persist_to_remove(cursor, to_remove)
            persist_last_changes(cursor, last_changes, today)



def _merge_rnb_diff():

    # Lancement de la réconciliation automatique des changements avec une connexion en rôle recserveur
    print("Preparation des tables de reconciliation")
    with get_connection_recserveur() as conn :
        with get_cursor(conn) as cursor:
            prepare_recserveur_tables_for_to_remove(cursor)
            prepare_recserveur_tables_for_last_changes(cursor)

    table_delete_deactivation = "delete_batiment_rnb_lien_bdtopo__rnb_deactivation"
    table_update = "update_batiment_rnb_lien_bdtopo__moissonnage"
    table_insert = "insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage"
    table_delete_demolished= "delete_batiment_rnb_lien_bdtopo__rnb_demolished"
    tables_poires = []
    demolished_poire = False

    with get_connection_recserveur() as conn :
        with get_cursor(conn) as cursor:

            if is_table_empty(cursor,"recserveur",table_delete_deactivation):
                print(table_delete_deactivation," vide")
            else:
                tables_poires.append(table_delete_deactivation)

            if is_table_empty(cursor,"recserveur",table_update):
                print(table_update," vide")
            else:
                tables_poires.append(table_update)

            if is_table_empty(cursor,"recserveur",table_insert):
                print(table_insert," vide")
            else:
                tables_poires.append(table_insert)
                
            if is_table_empty(cursor,"recserveur",table_delete_demolished):
                print(table_delete_demolished," vide")
            else:
                demolished_poire = True


    print("Reconciliations des tables : ", tables_poires)

    result = launch_poires(tables_poires)
    print("numrec MAJ batiment_rnb : ", result["numrec"])
    
    
    if demolished_poire:
        print("Reconciliation delete demolished")
        result = launch_poires(table_delete_demolished)
        print("numrec delete_demolished : ", result["numrec"])



if __name__ == "__main__":

    # sync_rnb_from_file("data/rnb_diff_2024-05-01.csv")  # 4.2 Go
    # sync_rnb_from_file("data/diff_2025-01-10.csv")  # 1.4 Go
    # sync_rnb_from_file("data/diff_2025-06-01.csv")  # 53 Mo
    # sync_rnb_from_file("data/diff_2026-07-29_with_users.csv")
    sync_rnb_from_file("data/27_juillet_diff_33063_a_partir_de-2026-03-01_filtre.csv") # 17 Mo

    # one_week_ago = datetime.now() - timedelta(days=2)
    # sync_rnb(one_week_ago)
