import pandas

communeCSV = pandas.read_csv("../data/communes-france-2024-limite.csv", sep=";", header=0)

print(communeCSV)

with open("../sql/data.sql", "w", encoding="utf-8") as data_file:
    print("Starting SQL file creation...")
    unique_regions = set()
    for row in communeCSV.itertuples():
        unique_regions.add((row.reg_code, row.reg_nom))
    for code, nom in unique_regions:
        data_file.write(f'INSERT INTO regions (code, nom) VALUES ("{code}", "{nom}");\n')
    print(f"Inserted {len(unique_regions)} unique regions.")
    unique_departements = set()
    for row in communeCSV.itertuples():
        unique_departements.add((row.dep_code, row.dep_nom, row.reg_code))
    for code, nom, reg_code in unique_departements:
        data_file.write(f'INSERT INTO departements (code, nom, region_code) VALUES ("{code}", "{nom}", "{reg_code}");\n')
    print(f"Inserted {len(unique_departements)} unique departements.")
    for row in communeCSV.itertuples():
        data_file.write(f'INSERT INTO communes (code_insee, nom, postal_code, departement_code) VALUES ("{row.code_insee}", "{row.nom_standard}", "{row.code_postal}", "{row.dep_code}");\n')
    print(f"Inserted {len(communeCSV)} communes.")
data_file.close()
print("Data SQL file created.")

