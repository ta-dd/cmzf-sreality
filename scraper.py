import sys
import sqlite3

sys.path.append("cmzf-redataprocessing/redataprocessing/src")

import redataprocessing as rdp

DB_NAME = "2024-03-25.sqlite"


def merge_tables(db_table_name):
    con = sqlite3.connect(DB_NAME)

    merged_table_name = "MERGED_" + db_table_name
    query = f"""
        CREATE TABLE IF NOT EXISTS {merged_table_name} AS
        SELECT o.*, d.*
        FROM {"OFFERS_" + db_table_name} o
        LEFT JOIN {"DESCRIPTION_" + db_table_name} d ON o.hash_id = d.hash_id
    """
    con.execute(query)
    con.commit()
    con.close()


kraje = [
    "Jihočeský kraj",
    "Plzeňský kraj",
    "Karlovarský kraj",
    "Ústecký kraj",
    "Liberecký kraj",
    "Královéhradecký kraj",
    "Pardubický kraj",
    "Olomoucký kraj",
    "Zlínský kraj",
    "Hlavní město Praha",
    "Středočeský kraj",
    "Moravskoslezský kraj",
    "Kraj Vysočina",
    "Jihomoravský kraj",
]

for kraj in kraje:
    print(f"-----------------------------------------")
    print(f"--- Downloading {kraj} ---")
    db_table_name = rdp.get_re_offers(
        path_to_sqlite=DB_NAME,
        category_main="landplots",
        category_type="sale",
        locality_region=[kraj],
    )
merge_tables(db_table_name=db_table_name)
