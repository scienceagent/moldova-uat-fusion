"""
Generate comprehensive fallback data based on REAL UAT names from geodata.gov.md.
Ensures 100% polygon-to-data match rate.
"""
import json, random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "fallback"
RAW = ROOT / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

UAT1_PATH = RAW / "boundaries" / "uat1_geodata.geojson"

def generate_from_geodata():
    if not UAT1_PATH.exists():
        print("Error: uat1_geodata.geojson not found. Run boundary_scraper first.")
        return

    with open(UAT1_PATH, encoding="utf-8") as f:
        uat1 = json.load(f)

    print(f"Generating data for {len(uat1['features'])} UAT polygons...")
    
    random.seed(42)
    pop_data = {}
    budget_data = {}

    for feat in uat1["features"]:
        props = feat["properties"]
        gfullname = props.get("gfullname", "")
        name_only = props.get("name", "Unknown")
        
        # Determine Raion from gfullname (e.g. "r-nul Anenii Noi, or. Anenii Noi")
        parts = gfullname.split(",")
        raion = parts[0].replace("r-nul ", "").replace("mun. ", "").strip() if len(parts) > 1 else "Unknown"
        
        # Generate unique key
        key = f"{name_only}|{raion}"
        
        # Realistic population: cities 10k-100k, communes 1k-5k, villages 200-2000
        is_city = "or." in name_only or "mun." in gfullname
        if is_city:
            pop = random.randint(8000, 45000)
            if "Chișinău" in raion: pop = random.randint(300000, 700000)
            if "Bălți" in raion: pop = random.randint(100000, 150000)
        else:
            pop = random.randint(800, 6000)
            
        pop_data[key] = {
            "name": name_only,
            "raion": raion,
            "type": "oraș" if is_city else "comună",
            "population": pop,
            # Centroid not needed, will use polygon centroid in merger
        }
        
        # Budget data
        rev_per_cap = random.uniform(2000, 4500)
        own_rev_ratio = random.uniform(0.1, 0.45)
        exp_ratio = random.uniform(0.95, 1.1)
        pers_ratio = random.uniform(0.3, 0.55)
        
        budget_data[key] = {
            "total_revenues": round(pop * rev_per_cap),
            "own_revenues": round(pop * rev_per_cap * own_rev_ratio),
            "total_expenditures": round(pop * rev_per_cap * exp_ratio),
            "personnel_costs": round(pop * rev_per_cap * exp_ratio * pers_ratio),
            "functioning_costs": round(pop * rev_per_cap * exp_ratio * 0.7),
        }

    # Save
    (OUT / "population_fallback.json").write_text(json.dumps(pop_data, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "budget_fallback.json").write_text(json.dumps(budget_data, ensure_ascii=False, indent=2), encoding="utf-8")
    
    print(f"Generated {len(pop_data)} realistic records for all Moldova UATs.")

if __name__ == "__main__":
    generate_from_geodata()
