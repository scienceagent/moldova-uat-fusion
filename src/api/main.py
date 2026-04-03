from fastapi import FastAPI
from pathlib import Path
import json

app = FastAPI(title="Moldova UAT Fusion API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/uat/master")
def uat_master():
    p = Path("data/processed/uat_master.json")
    if not p.exists():
        return {"error": "run pipeline first"}
    return json.loads(p.read_text(encoding="utf-8"))