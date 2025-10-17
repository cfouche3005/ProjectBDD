import pandas

communeCSV = pandas.read_csv("../data/communes-france-2024-limite.csv", sep=";", header=0)

print(communeCSV)

with open("../sql/geo.sql", "w", encoding="utf-8") as data_file:
    print("Starting SQL file creation...")
    unique_regions = set()
    for row in communeCSV.itertuples():
        unique_regions.add((row.reg_code, row.reg_nom))
    for code, nom in unique_regions:
        data_file.write(f"INSERT INTO regions (reg_code, nom_region) VALUES ('{code}', '{nom.replace("'", "''")}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_regions)} unique regions.")
    unique_departements = set()
    for row in communeCSV.itertuples():
        unique_departements.add((row.dep_code, row.dep_nom, row.reg_code))
    for code, nom, reg_code in unique_departements:
        data_file.write(f"INSERT INTO departements (num_departement, nom_departement, reg_code) VALUES ('{code}', '{nom.replace("'", "''")}', '{reg_code}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_departements)} unique departements.")
    for row in communeCSV.itertuples():
        data_file.write(f"INSERT INTO communes (code_insee, nom_commune, population, code_postal,num_departement) VALUES ('{row.code_insee}', '{row.nom_standard.replace("'", "''")}', '{row.population}', '{row.code_postal}', '{row.dep_code}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(communeCSV)} communes.")
data_file.close()
print("Data SQL file created.")

