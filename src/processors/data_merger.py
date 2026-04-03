"""
Data merger: combines population, budget, and UAT1 polygon boundaries into
unified processed files for the API and frontend.
"""
import json, logging, math
from pathlib import Path
import geopandas as gpd
from src.processors.normalize_uat import normalize_name

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
FALLBACK = ROOT / "data" / "fallback"
PROC = ROOT / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict | list:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def merge_all():
    """Merge population, budget, and UAT1 boundaries into processed output."""
    # Load population
    pop = load_json(RAW / "population" / "population_data.json")
    if not pop or "uat_population" in pop or not any("population" in str(v) for v in list(pop.values())[:1]):
        pop = load_json(FALLBACK / "population_fallback.json")
    logger.info(f"Population data: {len(pop)} UATs")

    # Load budget
    budget = load_json(RAW / "budget" / "budget_parsed.json")
    if not budget:
        budget = load_json(FALLBACK / "budget_fallback.json")
    logger.info(f"Budget data: {len(budget)} entries")

    # Load UAT1 polygons (from geodata.gov.md)
    uat1_path = RAW / "boundaries" / "uat1_geodata.geojson"
    if uat1_path.exists():
        logger.info(f"Loading and transforming UAT1 polygons from {uat1_path}")
        gdf = gpd.read_file(uat1_path)
        # Moldova's geodata.gov.md typically outputs in EPSG:4026 (MOLDREF99)
        # If no CRS is defined in the file, we set it manually before transforming.
        if gdf.crs is None:
            gdf.set_crs("EPSG:4026", allow_override=True, inplace=True)
        # Transform to WGS84 for web maps
        gdf = gdf.to_crs("EPSG:4326")
        uat1_features = json.loads(gdf.to_json())["features"]
    else:
        uat1_features = []
    
    logger.info(f"UAT1 polygon features: {len(uat1_features)}")

    # Load Raion boundaries (ADM1) for large scale view
    raions_path = RAW / "boundaries" / "adm1_geoboundaries.geojson"
    if not raions_path.exists():
        raions_path = RAW / "boundaries" / "adm1_hdx.geojson"
    raions = load_json(raions_path) if raions_path.exists() else {"type": "FeatureCollection", "features": []}

    # Index population/budget data by normalized name
    # We create a mapping: normalized_name -> original_key/data
    data_index = {}
    for key, pdata in pop.items():
        # Key is typically "Name|Raion", so normalize the key for matching
        norm_key = normalize_name(key)
        data_index[norm_key] = {
            "id": key,
            "pop_data": pdata,
            "budget_data": budget.get(key, {})
        }
    
    logger.info(f"Data index sample keys: {list(data_index.keys())[:10]}")

    # Build UAT master list with polygons
    polygon_features = []
    processed_ids = set()
    matches_found = 0
    misses = []
    
    # Iterate over features from UAT1 GeoJSON
    for feat in uat1_features:
        props = feat.get("properties", {})
        # Use gfullname which contains "r-nul X, or. Y" for robust matching
        gfullname = props.get("gfullname", props.get("name", ""))
        norm_name = normalize_name(gfullname)
        
        # Try to find a match in our data index
        match = data_index.get(norm_name)
        
        # If no exact match, try fuzzy (containing)
        if not match:
            for k in data_index:
                if k in norm_name or norm_name in k:
                    match = data_index[k]
                    break

        if match:
            matches_found += 1
            if matches_found <= 10:
                logger.info(f"MATCH: '{gfullname}' -> '{match['id']}'")
            uat_id = match["id"]
            pdata = match["pop_data"]
            bdata = match["budget_data"]
            processed_ids.add(uat_id)
            
            # Enrich the feature's properties
            # We keep the polygon geometry
            enriched_props = {
                "id": uat_id,
                "name": pdata.get("name", props.get("name", "Unknown")),
                "raion": pdata.get("raion", ""),
                "type": pdata.get("type", "comună"),
                "population": pdata.get("population", 0),
                "total_revenues": bdata.get("total_revenues", 0),
                "own_revenues": bdata.get("own_revenues", 0),
                "total_expenditures": bdata.get("total_expenditures", 0),
                "personnel_costs": bdata.get("personnel_costs", 0),
                "functioning_costs": bdata.get("functioning_costs", 0),
            }
            
            new_feat = {
                "type": "Feature",
                "geometry": feat["geometry"],
                "properties": enriched_props
            }
            polygon_features.append(new_feat)
        else:
            misses.append(gfullname)
            if len(misses) <= 10:
                logger.info(f"MISS: '{gfullname}' (norm: '{norm_name}')")

    # Master list of UATs (including those with and without polygons)
    uat_master_list = []
    for key, info in data_index.items():
        pdata = info["pop_data"]
        bdata = info["budget_data"]
        uat_master_list.append({
            "id": info["id"],
            "name": pdata.get("name", ""),
            "raion": pdata.get("raion", ""),
            "type": pdata.get("type", "comună"),
            "population": pdata.get("population", 0),
            "lat": pdata.get("lat", 47.0),
            "lon": pdata.get("lon", 28.8),
            "budget": bdata,
            "has_polygon": info["id"] in processed_ids
        })

    # Save outputs
    master = {
        "updated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "total_uats": len(uat_master_list),
        "total_population": sum(u["population"] for u in uat_master_list),
        "polygon_count": len(polygon_features),
        "sources": {
            "population": "RPL 2024 / BNS fallback",
            "budget": "dataset.gov.md / Ministry of Finance",
            "boundaries": "geodata.gov.md / geoBoundaries / HDX"
        },
        "uat": uat_master_list
    }

    (PROC / "uat_master.json").write_text(json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8")
    (PROC / "uat_polygons.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": polygon_features}, ensure_ascii=False), encoding="utf-8")
    (PROC / "boundaries_adm1.geojson").write_text(json.dumps(raions, ensure_ascii=False), encoding="utf-8")

    logger.info(f"Saved {len(uat_master_list)} UATs to master.json")
    logger.info(f"Saved {len(polygon_features)} polygons to uat_polygons.geojson")
    return master


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge_all()
