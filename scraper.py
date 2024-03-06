import sys

sys.path.append("redataprocessing/redataprocessing/src")

import redataprocessing as rdp

rdp.get_re_offers(
    path_to_sqlite="test7.sqlite",
    category_main="landplots",
    category_type="sale",
    locality_region=["Liberecký kraj"],
)

sys.exit()


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
    rdp.get_re_offers(
        path_to_sqlite="test2.sqlite",
        category_main="landplots",
        category_type="sale",
        locality_region=[kraj],
    )
