import redataprocessing as rdp

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
        path_to_sqlite="estate_data.sqlite2",
        category_main="landplots",
        category_type="sale",
        locality_region=[kraj],
    )
