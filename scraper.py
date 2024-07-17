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
            # TODO downloading offers could work (and get written to DB), then descriptions could fail, causing offers to be duplicated.
            # solution: rewrite rdp.get_re_offers logic OR write a function to deduplicate the sqlite db / pandas dataframe to keep only one record for each unique hash
            # if the description download is reading the list of offers from the DB, it may be easier to just deduplicate the resulting DB
            # or just add the retry decorator to the list downloading function
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
                f"Error downloading data for {kraj}: {str(e)}. Retrying in {delay} seconds..."
            )
            attempts += 1
            time.sleep(delay)
    print(
        f"\n\n\n--- Failed to download data for {kraj} after {max_retries} attempts ---\n\n\n"
    )


for kraj in kraje:
    download_kraj(kraj, DB_NAME, "landplots", "sale")

merge_tables(db_table_name=db_table_name)


# Connect to the SQLite database
conn = sqlite3.connect(DB_NAME)

# Query the table and load it into a DataFrame
df = pd.read_sql_query("SELECT * FROM MERGED_LANDPLOTS_SALE", conn)

df.to_csv(f"sreality_{DB_NAME.split('.')[0]}.csv", index=False)

conn.close()


# karly kod

# df_hist = pd.DataFrame(pgConn.execute_sql("select * from sandbox.sreality_history"))
# #df_new = pd.DataFrame(pgConn.execute_sql("select * from sandbox.sreality_" + date_download_name))
# df_new = pd.DataFrame(pgConn.execute_sql("select * from sandbox.sreality_current"))

# pgConn.close()

# date_download = df_new['date_download'].to_list()[0]
# date_download_name = date_download.replace('-','_')

# # koukni, jaké datumy jsou už appendnuté v datasetu
# df_hist.groupby('date_download').size()
# correct_schema_cols = df_hist.columns # get the correct order of columns
# df_new_corr = df_new[correct_schema_cols[1:]] # change the order of columns - without the 'index' column
# # df_sreality_history = pd.concat([df_new_corr, df_hist])

# if date_download not in set(df_hist['date_download'].to_list()):
#     pgConn.start()
#     pgConn.to_sql(df_new_corr, 'sreality_history', if_exists='append') # append historized table
#     pgConn.close()
