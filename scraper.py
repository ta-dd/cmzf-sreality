import sys
import sqlite3
from datetime import datetime
import time
import pandas as pd
import re

current_date = datetime.now().strftime("%Y-%m-%d")


sys.path.append("cmzf-redataprocessing/redataprocessing/src")

import redataprocessing as rdp

DB_NAME = f"{current_date}.sqlite"


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

db_table_name = None


def download_kraj(
    kraj, path_to_sqlite, category_main, category_type, max_retries=5, delay=3
):
    global db_table_name
    attempts = 0
    while attempts < max_retries:
        try:
            print(f"-----------------------------------------")
            print(f"--- Downloading {kraj} (Attempt {attempts + 1}/{max_retries}) ---")
            db_table_name = rdp.get_re_offers(
                path_to_sqlite=path_to_sqlite,
                category_main=category_main,
                category_type=category_type,
                locality_region=[kraj],
            )
            print(f"--- Successfully downloaded and committed data for {kraj} ---")
            return  # Exit the loop and function on success
        except Exception as e:
            print(
                f"Error downloading data for {kraj}: {str(e)}. Ran attempt #{attempts+1} out of {max_retries}. Retrying in {delay} seconds..."
            )
            attempts += 1
            time.sleep(delay)
    print(
        f"\n\n\n--- Failed to download data for {kraj} after {max_retries} attempts ---\n\n\n"
    )


for kraj in kraje:
    download_kraj(kraj, DB_NAME, "landplots", "sale", max_retries=1)

merge_tables(db_table_name=db_table_name)


conn = sqlite3.connect(DB_NAME)

df = pd.read_sql_query("SELECT * FROM MERGED_LANDPLOTS_SALE", conn)

df.to_csv(f"sreality_{DB_NAME.split('.')[0]}.csv", index=False)

conn.close()
