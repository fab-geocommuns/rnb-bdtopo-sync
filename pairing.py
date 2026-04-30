from db import get_connection, get_cursor


def run_pairing_after_rnb_update():

    # sql function: rnb_maj_liens_vers_batiment_car_maj_batiment_rnb
    with get_connection() as conn:
        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT processus_divers.rnb_maj_liens_vers_batiment_car_maj_batiment_rnb();"
            )
