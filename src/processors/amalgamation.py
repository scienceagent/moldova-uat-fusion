"""
Amalgamation algorithm for Moldova UATs using REAL POLYGONS.
Merges small UATs (below population threshold) with adjacent neighbors in same Raion.
Robust geometry handling with Shapely and GeoPandas.
"""
import json, logging, copy, math, sys
from pathlib import Path
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import shape
import pandas as pd

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "data" / "processed"

def run_amalgamation_polygons(threshold: int = 5000):
    path = PROC / "uat_polygons.geojson"
    if not path.exists():
        logger.error(f"{path} not found!")
        return None

    gdf = gpd.read_file(path)
    # Ensure index is unique and clean
    gdf = gdf.reset_index(drop=True)
    gdf['id_clean'] = gdf.index.astype(str)
    gdf = gdf.set_index('id_clean')
    
    # Fix invalid geometries
    gdf["geometry"] = gdf.geometry.buffer(0)
    
    original_gdf = gdf.copy()
    merges = {idx: [idx] for idx in gdf.index}
    merged_away = set()

    # Sort candidates
    small_uats_indices = gdf[
        (gdf["population"] < threshold) & 
        (gdf["type"] != "oraș")
    ].sort_values("population").index.tolist()

    logger.info(f"Threshold {threshold}: {len(small_uats_indices)} candidates.")

    for idx in small_uats_indices:
        if idx in merged_away: continue
        
        row = gdf.loc[idx]
        current_geom = row["geometry"]
        if current_geom is None or current_geom.is_empty: continue
        
        raion = row["raion"]
        potential_matches = gdf[
            (gdf["raion"] == raion) & 
            (~gdf.index.isin(merged_away)) &
            (gdf.index != idx)
        ]
        
        if potential_matches.empty: continue

        # Explicitly check for adjacency
        neighbors_mask = potential_matches.geometry.touches(current_geom)
        neighbors = potential_matches[neighbors_mask]
        
        best_neighbor_idx = None
        if not neighbors.empty:
            best_neighbor_idx = neighbors["population"].idxmax()
        else:
            # Nearest centroid
            dists = potential_matches.geometry.centroid.distance(current_geom.centroid)
            best_neighbor_idx = dists.idxmin()

        if best_neighbor_idx:
            merged_away.add(idx)
            merges[best_neighbor_idx].extend(merges[idx])
            
            # Attributes update
            for col in ["population", "total_revenues", "own_revenues", 
                        "total_expenditures", "personnel_costs", "functioning_costs"]:
                gdf.at[best_neighbor_idx, col] += row[col]
            
            # Geometry union
            try:
                g_target = gdf.at[best_neighbor_idx, "geometry"]
                combined = unary_union([g_target, current_geom])
                gdf.at[best_neighbor_idx, "geometry"] = combined.buffer(0)
            except Exception as e:
                logger.error(f"Union failed: {e}")
            
            # Merge name
            base_name = original_gdf.at[best_neighbor_idx, "name"]
            count = len(merges[best_neighbor_idx])
            gdf.at[best_neighbor_idx, "name"] = f"{base_name} (+{count-1})"
            gdf.at[best_neighbor_idx, "merged_count"] = count

    # Cleanup and Save
    final_gdf = gdf[~gdf.index.isin(merged_away)].copy()
    
    # Save statistics
    # Note: to_file doesn't like complex types in columns, so ensure simple dict conversion
    scenario_stats = {
        "threshold": threshold,
        "original_count": len(original_gdf),
        "result_count": len(final_gdf),
        "merged_count": len(merged_away),
        "uats": final_gdf.drop(columns='geometry').to_dict(orient='records')
    }
    
    final_gdf.to_file(PROC / f"amalgamated_{threshold}.geojson", driver="GeoJSON")
    with open(PROC / f"scenario_{threshold}.json", "w", encoding="utf-8") as f:
        json.dump(scenario_stats, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved scenario {threshold}: {len(final_gdf)} UATs")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for t in [3000, 5000]:
        run_amalgamation_polygons(threshold=t)
