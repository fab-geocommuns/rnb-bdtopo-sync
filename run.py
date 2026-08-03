import datetime
from datetime import datetime
from db import get_connection, get_connection_recserveur, get_cursor, create_last_changes_table, create_to_remove_table
from rnb import (
    getDiff_RNB_from_date,
    getDiff_RNB_from_file,
    convert_rnb_diff,
    persist_last_changes,
    persist_to_remove,
    prepare_recserveur_tables_for_last_changes,
    prepare_recserveur_tables_for_to_remove
)


def sync_rnb(since: datetime) -> tuple[list, set]:
    # Télécharger le fichier de diff
    rnb_diff = getDiff_RNB_from_date(since)

    _from_diff_to_db(rnb_diff)
     

def sync_rnb_from_file(filename: str) -> tuple[list, set]:
    rnb_diff = getDiff_RNB_from_file(filename)

    _from_diff_to_db(rnb_diff)
     


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

    # Lancement de la réconciliation automatique des changements avec une connexion en rôle recserveur
    print("Preparation des tables de reconciliation")
    with get_connection_recserveur() as conn :
        with get_cursor(conn) as cursor:
            prepare_recserveur_tables_for_to_remove(cursor)
            prepare_recserveur_tables_for_last_changes(cursor)

    



if __name__ == "__main__":

    # sync_rnb_from_file("data/rnb_diff_2024-05-01.csv")  # 4.2 Go
    # sync_rnb_from_file("data/diff_2025-01-10.csv")  # 1.4 Go
    sync_rnb_from_file("data/diff_2025-06-01.csv")  # 53 Mo
    # sync_rnb_from_file("data/27_juillet_diff_33063_a_partir_de-2026-03-01_filtre.csv") # 17 Mo

# one_week_ago = datetime.now() - timedelta(weeks=1)
# sync_rnb(one_week_ago)
