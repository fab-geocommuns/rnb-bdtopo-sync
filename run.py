import datetime
from datetime import datetime, timedelta
from rnb import (
    getDiff_RNB_from_date,
    getDiff_RNB_from_file,
    rnb_get_most_recent,
    calc_to_remove,
)


def sync_rnb(since: datetime) -> tuple[list, set]:

    # Télécharger le fichier de diff
    rnb_diff = getDiff_RNB_from_date(since)

    # On garde uniquement les lignes les plus récentes par rnb_id
    last_changes = rnb_get_most_recent(rnb_diff)

    # On tri entre les liens à casser ou les apapriemrnts à recalculer
    to_remove = calc_to_remove(last_changes)

    print(f"{len(to_remove)} éléments vont etre supprimés ")

    return last_changes, to_remove

    # On a nos données. On est prêts à insérer en base.

    # 1. Mettre à jour le stock RNB
    # 2. Remplir la table de liens à supprimer
    # 3. Remplir la table de liens à recalculer


def sync_rnb_from_file(filename: str) -> tuple[list, set]:

    rnb_diff = getDiff_RNB_from_file(filename)

    last_changes = rnb_get_most_recent(rnb_diff)

    to_remove = calc_to_remove(last_changes)

    return last_changes, to_remove


if __name__ == "__main__":

    sync_rnb_from_file("data/rnb_diff_2024-05-01.csv")  # 4.2 Go
    # sync_rnb_from_file("data/diff_2025-01-10.csv")  # 1.4 Go
    # sync_rnb_from_file("data/diff_2025-06-01.csv")  # 53 Mo

    # one_week_ago = datetime.now() - timedelta(weeks=1)
    # sync_rnb(one_week_ago)
