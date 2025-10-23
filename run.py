import datetime
from datetime import datetime, timedelta
from rnb import (
    getDiff_RNB_from_date,
    getDiff_RNB_from_file,
    rnb_get_most_recent,
    dispatch_rows,
)


def sync_rnb(since: datetime):

    # Télécharger le fichier de diff
    rnb_diff = getDiff_RNB_from_date(since)

    # On garde uniquement les lignes les plus récentes par rnb_id
    rnb_diff = rnb_get_most_recent(rnb_diff)

    # On tri entre les liens à casser ou les apapriemrnts à recalculer
    to_remove, to_calculate = dispatch_rows(rnb_diff)

    # On a nos données. On est prêts à insérer en base.

    # 1. Mettre à jour le stock RNB
    # 2. Remplir la table de liens à supprimer
    # 3. Remplir la table de liens à recalculer


def sync_rnb_from_file(filename: str):

    rnb_diff = getDiff_RNB_from_file(filename)

    rnb_diff = rnb_get_most_recent(rnb_diff)

    to_remove, to_calculate = dispatch_rows(rnb_diff)


if __name__ == "__main__":

    sync_rnb_from_file("data/diff_2024-05-01.csv")  # 4.2 Go
    # sync_rnb_from_file("data/diff_2025-01-10.csv")  # 1.4 Go
    # sync_rnb_from_file("data/diff_2025-06-01.csv") 53 Mo

    # one_week_ago = datetime.now() - timedelta(weeks=1)
    # sync_rnb(one_week_ago)
