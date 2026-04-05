"""Moldova UAT Fusion API — FastAPI backend."""
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pathlib import Path
import json, copy

app = FastAPI(title="Moldova UAT Fusion API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA = Path(__file__).resolve().parents[2] / "data" / "processed"


def _load(name: str):
    p = DATA / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/uat/master")
def uat_master():
    data = _load("uat_master.json")
    if not data:
        return {"error": "Run pipeline first"}
    return data


@app.get("/uat/list")
def uat_list(
    raion: str = Query(None),
    type: str = Query(None),
    min_pop: int = Query(None),
    max_pop: int = Query(None),
    sort_by: str = Query("population"),
    limit: int = Query(100),
    offset: int = Query(0),
):
    """Paginated, filterable UAT list."""
    data = _load("uat_master.json")
    if not data:
        return {"error": "No data"}
    uats = data.get("uat", [])
    if raion:
        uats = [u for u in uats if raion.lower() in u.get("raion", "").lower()]
    if type:
        uats = [u for u in uats if u.get("type", "") == type]
    if min_pop is not None:
        uats = [u for u in uats if u.get("population", 0) >= min_pop]
    if max_pop is not None:
        uats = [u for u in uats if u.get("population", 0) <= max_pop]

    reverse = sort_by != "name"
    uats.sort(key=lambda u: u.get(sort_by, 0), reverse=reverse)

    return {
        "total": len(uats),
        "offset": offset,
        "limit": limit,
        "items": uats[offset:offset+limit]
    }


@app.get("/uat/{uat_id}")
def uat_detail(uat_id: str):
    data = _load("uat_master.json")
    if not data:
        return {"error": "No data"}
    for u in data.get("uat", []):
        if u["id"] == uat_id:
            return u
    return {"error": "UAT not found"}


@app.get("/uat/boundaries/polygons")
def uat_polygons():
    """UAT polygon boundaries with all data."""
    data = _load("uat_polygons.geojson")
    return data or {"type": "FeatureCollection", "features": []}


@app.get("/uat/boundaries/raioane")
def raioane_polygons():
    """Raion border polygons."""
    data = _load("raioane_polygons.geojson")
    return data or {"type": "FeatureCollection", "features": []}


@app.get("/uat/boundaries/points")
def uat_points():
    """UAT point locations (centroids) fallback."""
    # Try polygons first, frontend handles both
    data = _load("uat_polygons.geojson")
    return data or {"type": "FeatureCollection", "features": []}


@app.get("/uat/boundaries/amalgamated")
def amalgamated_boundaries(threshold: int = Query(5000)):
    """Amalgamated UAT polygons for a given threshold."""
    data = _load(f"amalgamated_{threshold}.geojson")
    if not data:
        # Fallback to current if threshold not found
        return _load("uat_polygons.geojson")
    return data


@app.get("/uat/scenario/{threshold}")
def scenario(threshold: int):
    """Full scenario data with stats."""
    data = _load(f"scenario_{threshold}.json")
    if not data:
        return {"error": f"No scenario for threshold {threshold}"}
    return data


@app.get("/uat/stats")
def stats():
    """Aggregate statistics."""
    master = _load("uat_master.json")
    if not master:
        return {"error": "No data"}
    uats = master.get("uat", [])
    pops = [u["population"] for u in uats]
    below_3k = sum(1 for p in pops if p < 3000)
    below_5k = sum(1 for p in pops if p < 5000)

    s5 = _load("scenario_5000.json")
    s3 = _load("scenario_3000.json")

    return {
        "total_uats": len(uats),
        "total_population": sum(pops),
        "avg_population": round(sum(pops) / len(pops)) if pops else 0,
        "median_population": sorted(pops)[len(pops)//2] if pops else 0,
        "below_3000": below_3k,
        "below_5000": below_5k,
        "raions": len(set(u["raion"] for u in uats)),
        "scenario_5000_count": s5["result_count"] if s5 else None,
        "scenario_3000_count": s3["result_count"] if s3 else None,
    }


@app.post("/uat/merge")
async def custom_merge(request: dict):
    """Merge a custom set of UAT IDs on the fly."""
    from src.processors.amalgamation import haversine_km
    uat_ids = request.get("uat_ids", [])
    if len(uat_ids) < 2:
        return {"error": "Need at least 2 UAT IDs"}

    master = _load("uat_master.json")
    if not master:
        return {"error": "No data"}

    selected = [u for u in master["uat"] if u["id"] in uat_ids]
    if len(selected) < 2:
        return {"error": "Not enough matching UATs"}

    # Merge into the largest
    selected.sort(key=lambda u: u["population"], reverse=True)
    primary = copy.deepcopy(selected[0])
    members = [primary["id"]]
    for s in selected[1:]:
        primary["population"] += s["population"]
        primary["budget"]["total_revenues"] += s["budget"]["total_revenues"]
        primary["budget"]["own_revenues"] += s["budget"]["own_revenues"]
        primary["budget"]["total_expenditures"] += s["budget"]["total_expenditures"]
        primary["budget"]["personnel_costs"] += s["budget"]["personnel_costs"]
        primary["budget"]["functioning_costs"] += s["budget"]["functioning_costs"]
        members.append(s["id"])

    primary["name"] = f"{primary['name']} (+{len(members)-1})"
    primary["members"] = members
    primary["merged_count"] = len(members)

    # Estimate savings: sum of functioning costs of absorbed minus largest
    savings = sum(s["budget"]["functioning_costs"] for s in selected[1:])

    return {"merged_uat": primary, "estimated_savings": savings}