import pandas

dataCSV = pandas.read_csv("../data/data_cleaned.csv", sep=",", header=0)

print(dataCSV)

with open("../sql/data.sql", "w", encoding="utf-8") as data_file:
    print("Starting SQL file creation...")
    unique_modeles_onduleur = set()
    unique_marques_onduleur = set()
    unique_modeles_panneaux = set()
    unique_marques_panneaux = set()
    unique_installateurs = set()
    for row in dataCSV.itertuples():
        unique_modeles_onduleur.add(row.onduleur_modele)
        unique_marques_onduleur.add(row.onduleur_marque)
        unique_modeles_panneaux.add(row.panneaux_modele)
        unique_marques_panneaux.add(row.panneaux_marque)
        unique_installateurs.add(str(row.installateur))

    for modele in unique_modeles_onduleur:
        data_file.write(f"INSERT INTO mod_onduleur (nom_mod_ond) VALUES ('{modele.replace("'", "''")}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_modeles_onduleur)} unique inverter models.")

    for marque in unique_marques_onduleur:
        data_file.write(f"INSERT INTO marque_onduleur (nom_marque_ond) VALUES ('{marque.replace("'", "''")}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_marques_onduleur)} unique inverter brands.")

    for row in dataCSV.itertuples():
        data_file.write(
            f"INSERT INTO onduleur (nb_onduleur, id_mod_onduleur, id_marque_onduleur) SELECT '{row.nb_onduleur}', (SELECT id FROM mod_onduleur WHERE nom_mod_ond = '{row.onduleur_modele.replace("'", "''")}'), (SELECT id FROM marque_onduleur WHERE nom_marque_ond = '{row.onduleur_marque.replace("'", "''")}') WHERE NOT EXISTS (SELECT 1 FROM onduleur WHERE nb_onduleur = '{row.nb_onduleur}' AND id_mod_onduleur = (SELECT id FROM mod_onduleur WHERE nom_mod_ond = '{row.onduleur_modele.replace("'", "''")}') AND id_marque_onduleur = (SELECT id FROM marque_onduleur WHERE nom_marque_ond = '{row.onduleur_marque.replace("'", "''")}'));\n")
    data_file.write(f'\n')
    print(f"Inserted {len(dataCSV)} inverters (duplicates skipped).")

    for modele in unique_modeles_panneaux:
        data_file.write(f"INSERT INTO mod_panneau (nom_mod_pan) VALUES ('{modele.replace("'", "''")}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_modeles_panneaux)} unique panel models.")

    for marque in unique_marques_panneaux:
        data_file.write(f"INSERT INTO marque_panneau (nom_marque_pan) VALUES ('{marque.replace("'", "''")}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_marques_panneaux)} unique panel brands.")

    for row in dataCSV.itertuples():
        data_file.write(
            f"INSERT INTO panneau (nb_panneau, id_mod_panneau, id_marque_panneau) SELECT '{row.nb_panneaux}', (SELECT id FROM mod_panneau WHERE nom_mod_pan = '{row.panneaux_modele.replace("'", "''")}'), (SELECT id FROM marque_panneau WHERE nom_marque_pan = '{row.panneaux_marque.replace("'", "''")}') WHERE NOT EXISTS (SELECT 1 FROM panneau WHERE nb_panneau = '{row.nb_panneaux}' AND id_mod_panneau = (SELECT id FROM mod_panneau WHERE nom_mod_pan = '{row.panneaux_modele.replace("'", "''")}') AND id_marque_panneau = (SELECT id FROM marque_panneau WHERE nom_marque_pan = '{row.panneaux_marque.replace("'", "''")}'));\n")
    data_file.write(f'\n')
    print(f"Inserted {len(dataCSV)} panels (duplicates skipped).")

    for installateur in unique_installateurs:
        data_file.write(f"INSERT INTO installateur (nom_installateur) VALUES ('{installateur.replace("'", "''")}');\n")
    data_file.write(f'\n')
    print(f"Inserted {len(unique_installateurs)} unique installers.")

    for row in dataCSV.itertuples():
        data_file.write(f"INSERT INTO installation_pv (id,date_installation,pente,pente_opt,orientation,orientation_opt,surface,puissance_crete,production_pvgis,latitude,longitude,id_installateur,code_insee,id_onduleur,id_panneau) VALUES ('{row.id}','{row.date}','{row.pente}','{row.pente_optimum}','{row.orientation}','{row.orientation_optimum}','{row.surface}','{row.puissance_crete}','{int(row.production_pvgis)}','{row.lat}','{row.lon}',(SELECT id FROM installateur WHERE nom_installateur = '{str(row.installateur).replace("'", "''")}'),'{row.code_insee}',(SELECT id FROM onduleur WHERE nb_onduleur = '{row.nb_onduleur}' AND id_mod_onduleur = (SELECT id FROM mod_onduleur WHERE nom_mod_ond = '{row.onduleur_modele.replace("'", "''")}') AND id_marque_onduleur = (SELECT id FROM marque_onduleur WHERE nom_marque_ond = '{row.onduleur_marque.replace("'", "''")}')),(SELECT id FROM panneau WHERE nb_panneau = '{row.nb_panneaux}' AND id_mod_panneau = (SELECT id FROM mod_panneau WHERE nom_mod_pan = '{row.panneaux_modele.replace("'", "''")}') AND id_marque_panneau = (SELECT id FROM marque_panneau WHERE nom_marque_pan = '{row.panneaux_marque.replace("'", "''")}')));\n")
    data_file.write(f'\n')
    print(f"Inserted {len(dataCSV)} installations.")

data_file.close()
print("Data SQL file created.")