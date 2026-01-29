import datetime
from datetime import datetime
from rnb import (
    getDiff_RNB_from_date,
    getDiff_RNB_from_file,
    rnb_get_most_recent,
    calc_to_remove,
    persist_last_changes,
    persist_to_remove,
)


def sync_rnb(since: datetime) -> tuple[list, set]:
    # Télécharger le fichier de diff
    rnb_diff = getDiff_RNB_from_date(since)

    _from_diff_to_db(rnb_diff)


def sync_rnb_from_file(filename: str) -> tuple[list, set]:
    rnb_diff = getDiff_RNB_from_file(filename)

    _from_diff_to_db(rnb_diff)


def _from_diff_to_db(diff):

    last_changes, to_remove = _convert_rnb_diff(diff)

    # Insert last_changes and to_remove in the database
    persist_to_remove(to_remove)
    persist_last_changes(last_changes)


def _convert_rnb_diff(diff):
    last_changes = rnb_get_most_recent(diff)

    to_remove = calc_to_remove(last_changes)

    return last_changes, to_remove


if __name__ == "__main__":

    # sync_rnb_from_file("data/rnb_diff_2024-05-01.csv")  # 4.2 Go
    # sync_rnb_from_file("data/diff_2025-01-10.csv")  # 1.4 Go
    sync_rnb_from_file("data/diff_2025-06-01.csv")  # 53 Mo

# one_week_ago = datetime.now() - timedelta(weeks=1)
# sync_rnb(one_week_ago)
