import pandas
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from tqdm import tqdm

communeCSV = pandas.read_csv("../data/communes-france-2024-limite.csv", sep=";", header=0)
dataCSV = pandas.read_csv("../data/data_fixed.csv", sep=",", header=0)
dataCSV['orientation'] = dataCSV['orientation'].replace('Sud', 0)
dataCSV['orientation'] = dataCSV['orientation'].replace('South', 0)
dataCSV['installateur'] = dataCSV['installateur'].str.replace('"', '', regex=False)
median_orientation_optimum = dataCSV['orientation_optimum'].median()
dataCSV['orientation_optimum'] = dataCSV['orientation_optimum'].fillna(median_orientation_optimum)
median_pente_optimum = dataCSV['pente_optimum'].median()
dataCSV['pente_optimum'] = dataCSV['pente_optimum'].fillna(median_pente_optimum)


print(f"Initial data: {len(dataCSV)} rows")
columns_to_check = [col for col in dataCSV.columns if col != 'id']
dataCSV = dataCSV.drop_duplicates(subset=columns_to_check, keep='first')
print(f"After removing duplicates: {len(dataCSV)} rows remaining")

stats_lock = Lock()

def indexe_nom(nom):
    nom = nom.lower()
    nom = nom.replace("'", "")
    nom = nom.replace("-", "")
    nom = nom.replace(" ", "")
    nom = nom.replace("é", "e")
    nom = nom.replace("è", "e")
    nom = nom.replace("ê", "e")
    nom = nom.replace("à", "a")
    nom = nom.replace("â", "a")
    nom = nom.replace("î", "i")
    nom = nom.replace("ï", "i")
    nom = nom.replace("ô", "o")
    nom = nom.replace("ö", "o")
    nom = nom.replace("û", "u")
    nom = nom.replace("ü", "u")
    nom = nom.replace("ç", "c")
    return nom


# Créer des index optimisés pour la recherche
print("Building search indexes...")
communeCSV['nom_indexe'] = communeCSV['nom_standard'].apply(indexe_nom)
communeCSV['code_postal_int'] = communeCSV['code_postal'].astype(int)

# Index exact : (nom_indexe, code_postal) -> code_insee
exact_match_index = {}
for row in communeCSV.itertuples():
    key = (row.nom_indexe, row.code_postal_int)
    if key not in exact_match_index:
        exact_match_index[key] = row.code_insee

# Index partiel : code_postal -> [(nom_indexe, code_insee)]
partial_match_index = {}
for row in communeCSV.itertuples():
    if row.code_postal_int not in partial_match_index:
        partial_match_index[row.code_postal_int] = []
    partial_match_index[row.code_postal_int].append((row.nom_indexe, row.code_insee))

print("Indexes built.")


def get_insee_from_long_lat_api(latitude, longitude):
    url = f"https://geo.api.gouv.fr/communes?lat={latitude}&lon={longitude}&fields=nom,code,codesPostaux,anciensCodes&format=json&geometry=centre"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data[0]
    return None


def find_insee_local(nom, code_postal, stats):
    code_postal_str = str(int(code_postal))
    if len(code_postal_str) < 5:
        code_postal_str = "0" + code_postal_str
    code_postal_int = int(code_postal_str)

    nom_indexe = indexe_nom(nom)

    # Recherche exacte
    key = (nom_indexe, code_postal_int)
    if key in exact_match_index:
        with stats_lock:
            stats['exact_match'] += 1
        return exact_match_index[key], "exact"

    # Recherche partielle
    if code_postal_int in partial_match_index:
        for commune_nom, code_insee in partial_match_index[code_postal_int]:
            if nom_indexe in commune_nom:
                with stats_lock:
                    stats['partial_match'] += 1
                return code_insee, "partial"

    return None, None


def process_row_local(row, stats):
    insee, match = find_insee_local(row.locality, row.postal_code, stats)
    if insee is None:
        return (row.Index, None, row)

    date = f"{int(row.an_installation):04d}-{int(row.mois_installation):02d}-01"
    line = f'"{row.id}","{date}","{row.nb_panneaux}","{row.panneaux_marque}","{row.panneaux_modele}","{row.nb_onduleur}","{row.onduleur_marque}","{row.onduleur_modele}","{row.puissance_crete}","{row.surface}","{row.pente}","{row.pente_optimum}","{row.orientation}","{row.orientation_optimum}","{row.installateur}","{row.production_pvgis}","{row.lat}","{row.lon}","{insee}","{match}"\n'
    return (row.Index, line, None)


def process_row_api(row, stats):
    insee_data = get_insee_from_long_lat_api(row.lat, row.lon)

    if insee_data is None:
        print(
            f"Warning: (ID: {row.id}) INSEE code not found for {row.locality} with postal code {str(int(row.postal_code))}")
        with stats_lock:
            stats['failed'] += 1
            stats['no_insee'] += 1
        return None

    # Vérifier si le code INSEE existe dans communeCSV
    insee_code = insee_data['code']
    valid_insee_codes = set(communeCSV['code_insee'])

    if insee_code not in valid_insee_codes:
        # Chercher dans les anciens codes
        if 'anciensCodes' in insee_data and insee_data['anciensCodes']:
            found_valid_code = False
            for ancien_code in insee_data['anciensCodes']:
                if ancien_code in valid_insee_codes:
                    insee_code = ancien_code
                    found_valid_code = True
                    break

            if not found_valid_code:
                print(
                    f"Warning: (ID: {row.id}) No valid INSEE code found in anciensCodes (current: {insee_data['code']}, old: {insee_data['anciensCodes']})")
                with stats_lock:
                    stats['failed'] += 1
                    stats['no_ancien_code'] += 1
                return None
        else:
            print(
                f"Warning: (ID: {row.id}) No valid INSEE code found and no anciensCodes available (current: {insee_data['code']})")
            with stats_lock:
                stats['failed'] += 1
                stats['no_ancien_code'] += 1
            return None

    with stats_lock:
        stats['api_match'] += 1

    name_match = indexe_nom(row.locality) == indexe_nom(insee_data['nom'])
    postal_match = str(int(row.postal_code)) in insee_data['codesPostaux']

    if name_match:
        match_type = "api_city"
    elif postal_match:
        match_type = "api_postal"
    else:
        match_type = "api_none"

    date = f"{int(row.an_installation):04d}-{int(row.mois_installation):02d}-01"
    line = f'"{row.id}","{date}","{row.nb_panneaux}","{row.panneaux_marque}","{row.panneaux_modele}","{row.nb_onduleur}","{row.onduleur_marque}","{row.onduleur_modele}","{row.puissance_crete}","{row.surface}","{row.pente}","{row.pente_optimum}","{row.orientation}","{row.orientation_optimum}","{row.installateur}","{row.production_pvgis}","{row.lat}","{row.lon}","{insee_code}","{match_type}"\n'

    return (row.Index, line)


print("Starting CSV file creation...")
start_time = time.time()

stats = {
    'exact_match': 0,
    'partial_match': 0,
    'api_match': 0,
    'failed': 0,
    'no_insee': 0,
    'no_ancien_code': 0
}


results = {}
rows_needing_api = []

# Phase 1: Traitement local multithread
print("Phase 1: Processing local matches...")
with ThreadPoolExecutor(max_workers=2048) as executor:
    futures = {executor.submit(process_row_local, row, stats): row for row in dataCSV.itertuples()}

    for future in tqdm(as_completed(futures), total=len(dataCSV), desc="Local processing", unit="row"):
        idx, line, row = future.result()
        if line is not None:
            results[idx] = line
        else:
            rows_needing_api.append(row)

# Phase 2: Traitement API avec limite de 45 req/sec
if rows_needing_api:
    print(f"\nPhase 2: Processing API matches for {len(rows_needing_api)} rows...")
    max_workers = 45

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_row_api, row, stats): row for row in rows_needing_api}

        for future in tqdm(as_completed(futures), total=len(rows_needing_api), desc="API processing", unit="row"):
            result = future.result()
            if result is not None:
                idx, line = result
                results[idx] = line
            time.sleep(1 / 45)

with open("../data/data_cleaned.csv", "w", encoding="utf-8") as data_file:
    data_file.write(
        f'"id","date","nb_panneaux","panneaux_marque","panneaux_modele","nb_onduleur","onduleur_marque","onduleur_modele","puissance_crete","surface","pente","pente_optimum","orientation","orientation_optimum","installateur","production_pvgis","lat","lon","code_insee","match"\n')

    for idx in sorted(results.keys()):
        data_file.write(results[idx])

end_time = time.time()
execution_time = end_time - start_time

print(f"\nExact matches: {stats['exact_match']}")
print(f"Partial matches: {stats['partial_match']}")
print(f"API matches: {stats['api_match']}")
print(f"Failed: {stats['failed']}")
print(f"  - API returned no INSEE code: {stats['no_insee']}")
print(f"  - No valid INSEE code in anciensCodes: {stats['no_ancien_code']}")
print(f"Total processed: {len(dataCSV)}")
print(f"Execution time: {execution_time:.2f} seconds")

