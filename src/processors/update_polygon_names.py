import json
import pandas as pd
import unidecode
import re
import difflib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def clean_name(n):
    n = str(n).replace('\n', ' ').strip()
    # remove footnote markers like ³
    n = re.sub(r'[³]', '', n)
    n = n.replace('din care pe sectoare', '')
    n = n.strip(', ')
    # remove prefixes
    n = re.sub(r'^(com\.|or\.|sat\.|mun\.|r-nul|raionul)\s+', '', n, flags=re.IGNORECASE)
    n = re.sub(r'\br-nul\b', '', n, flags=re.IGNORECASE).strip()
    return n

def norm(n):
    return unidecode.unidecode(clean_name(n)).lower()

def run():
    print("Loading Excel...")
    xl = pd.ExcelFile(ROOT / 'data/raw/Anexa_Localitati_RPL2024.xlsx')
    df = pd.read_excel(xl, sheet_name='8.3', skiprows=4)
    
    col_raion_tip = df.columns[2]
    col_name = df.columns[3]
    col_pop = df.columns[4]

    # Map Raion context
    current_raion = None
    census_uats = {} # raion_norm -> { name_norm: (original_name, pop) }

    for _, row in df.iterrows():
        tip = str(row[col_raion_tip])
        name = str(row[col_name])
        pop = row[col_pop]
        
        if tip == 'Raioane':
            current_raion = norm(name)
            census_uats[current_raion] = {}
        elif tip == 'Comune' and current_raion:
            n_norm = norm(name)
            census_uats[current_raion][n_norm] = {
                'original_name': name,
                'population': pop
            }

    # Custom mapping for Chisinau
    if norm('Mun. Chișinău') in census_uats:
        chi_key = norm('Mun. Chișinău')
        # Geojson calls it "mun. Chișinău", excel calls it "or. Chișinău, \ndin care pe sectoare³"
        chi_city_excel = None
        for k in census_uats[chi_key]:
            if 'chisinau' in k:
                chi_city_excel = census_uats[chi_key][k]['original_name']
                break
        if chi_city_excel:
            census_uats[chi_key][norm('Chișinău')] = census_uats[chi_key][norm(chi_city_excel)]

    print(f"Loaded {sum(len(v) for v in census_uats.values())} communes from Census.")

    print("Census Raions (norm):", list(census_uats.keys())[:10])

    print("Loading GeoJSON...")
    geo_path = ROOT / 'data/raw/boundaries/uat1_geodata.geojson'
    with open(geo_path, encoding='utf-8') as f:
        geo = json.load(f)

    matches = 0
    mismatches = 0
    updated_features = []

    for i, feat in enumerate(geo['features']):
        props = feat['properties']
        gfullname = props.get('gfullname', '')
        geo_name = props.get('name', '')
        
        raion_part = gfullname.split(',')[0]
        r_norm = norm(raion_part)
        
        if i < 5:
            print(f"Geo Raion norm (first 5): {r_norm} from '{gfullname}'")
        
        # fallback for chisinau
        if 'chisinau' in r_norm or 'chisinau' in norm(gfullname):
            r_norm = norm('Mun. Chișinău')
        if 'balti' in r_norm or 'balti' in norm(gfullname):
            r_norm = norm('Mun. Bălți')
            
        n_norm = norm(geo_name)
        
        match_found = False
        if r_norm in census_uats:
            raion_dict = census_uats[r_norm]
            if n_norm in raion_dict:
                # exact match after normalization
                exact = raion_dict[n_norm]
                props['name'] = clean_name(exact['original_name'])
                match_found = True
            else:
                # try fuzzy match
                candidates = list(raion_dict.keys())
                closest = difflib.get_close_matches(n_norm, candidates, n=1, cutoff=0.7)
                if closest:
                    exact = raion_dict[closest[0]]
                    props['name'] = clean_name(exact['original_name'])
                    match_found = True
                else:
                    # special cases
                    for cand in candidates:
                        if n_norm in cand or cand in n_norm:
                            exact = raion_dict[cand]
                            props['name'] = clean_name(exact['original_name'])
                            match_found = True
                            break

        if match_found:
            matches += 1
        else:
            mismatches += 1
            print(f"NO MATCH: r_norm={r_norm}, n_norm={n_norm}, geo_name={geo_name}")
            
        feat['properties'] = props
        updated_features.append(feat)

    print(f"Matches: {matches}, Mismatches: {mismatches}")
    geo['features'] = updated_features
    
    out_path = ROOT / 'data/raw/boundaries/uat1_geodata_fixed.geojson'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(geo, f, ensure_ascii=False)
    print(f"Saved fixed GeoJSON to {out_path}")

if __name__ == '__main__':
    run()
