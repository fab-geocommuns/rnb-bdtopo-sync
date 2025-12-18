from db import get_cursor

with get_cursor() as cursor:

    q = "SELECT * FROM pg_available_extensions WHERE name = 'postgis';"
    cursor.execute(q)
    print(cursor.fetchall())
