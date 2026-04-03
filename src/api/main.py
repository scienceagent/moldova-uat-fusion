from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json

app = FastAPI(title="Moldova UAT Fusion API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/uat/master")
def uat_master():
    p = Path("data/processed/uat_master.json")
    if not p.exists():
        return {"error": "run pipeline first"}
    return json.loads(p.read_text(encoding="utf-8"))

@app.get("/uat/boundaries")
def uat_boundaries():
    p = Path("data/processed/boundaries.geojson")
    if not p.exists():
        return {"type": "FeatureCollection", "features": []}
    return json.loads(p.read_text(encoding="utf-8"))